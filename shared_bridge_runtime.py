"""Serialized RackAI reservation and operator handoff state for the shared bridge."""
import json
import queue
import threading
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

ALIAS = "rackai-service"
CONTRACT = "rack-ai/inference-service/v1"
TERMINAL = {"released", "cancelled", "expired", "preempted", "unavailable", "recovery_required"}
PREPARE_TIMEOUT_SECONDS = 120
QUEUE_CAPACITY = 64


class HandoffState(str, Enum):
    IDLE = "idle"
    RELEASING = "releasing_previous"
    RESERVING = "reserving_new"
    PREPARING = "preparing"
    READY = "ready"
    FAILED = "failed"


@dataclass
class Job:
    client: str
    endpoint: str
    body: bytes
    service: str
    request_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    done: threading.Event = field(default_factory=threading.Event)
    finished: threading.Event = field(default_factory=threading.Event)
    result: object = None
    error: str | None = None
    cancelled: bool = False


@dataclass
class SelectionState:
    selected_service: str
    previous_service: str | None = None
    handoff_state: HandoffState = HandoffState.IDLE
    failure_phase: str | None = None
    failure_reason: str | None = None
    reservation_id: str | None = None
    reservation_service: str | None = None

    def public(self):
        return {
            "selected_service": self.selected_service,
            "previous_service": self.previous_service,
            "handoff_state": self.handoff_state.value,
            "failure_phase": self.failure_phase,
            "failure_reason": self.failure_reason,
            "reservation_id": self.reservation_id,
            "reservation_service": self.reservation_service,
        }


class StateStore:
    def __init__(self, path):
        self.path = Path(path)

    def read(self):
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}

    def write(self, state):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(self.path.name + "." + uuid.uuid4().hex + ".tmp")
        temporary.write_text(json.dumps(state.public(), sort_keys=True), encoding="utf-8")
        temporary.replace(self.path)


def discover_profiles(call):
    result = call({"operation": "discover"})
    profiles = {}
    for descriptor in result.get("tags", []):
        contract = descriptor.get("inference_service", {})
        tag = descriptor.get("tag")
        if (
            isinstance(tag, str)
            and descriptor.get("qualified") is True
            and contract.get("contract") == CONTRACT
            and contract.get("model_alias") == ALIAS
        ):
            profiles[tag] = descriptor
    if not profiles:
        raise RuntimeError("RackAI discovery returned no qualified inference services")
    return profiles


class Bridge:
    def __init__(self, runtime_call, profiles, state_store=None):
        self.call = runtime_call
        self.profiles = profiles
        initial = "mini-brain" if "mini-brain" in profiles else next(iter(profiles))
        self.state = SelectionState(initial)
        self.store = state_store or StateStore("/home/tomp/.local/share/rackai-shared-bridge/state.json")
        self.lock = threading.RLock()
        self.changed = threading.Condition(self.lock)
        self.activation_lock = threading.Lock()
        self.jobs = queue.Queue(maxsize=QUEUE_CAPACITY)
        self.pending = Counter()
        self.active = None
        self.switching = False
        self._restore()
        self.worker = threading.Thread(target=self._run, daemon=True)
        self.worker.start()

    @property
    def selected(self):
        return self.state.selected_service

    @property
    def reservation(self):
        if not self.state.reservation_id:
            return None
        return {"id": self.state.reservation_id, "service": self.state.reservation_service}

    @reservation.setter
    def reservation(self, value):
        self.state.reservation_id = value and value.get("id")
        self.state.reservation_service = value and value.get("service")

    def status(self):
        with self.lock:
            result = self.state.public()
            result.update({
                "active_client": self.active.client if self.active else None,
                "queue_length": self.jobs.qsize(),
                "target_service": self.active.service if self.active else self.selected,
                "services": self.profiles,
                "reservation_state": self.state.handoff_state.value,
                "last_error": self.state.failure_reason,
            })
            return result

    def submit(self, client, endpoint, body):
        with self.lock:
            service = self.selected
        self._validate(service, body)
        job = Job(client, endpoint, body, service)
        with self.changed:
            self.pending[service] += 1
        try:
            self.jobs.put_nowait(job)
        except queue.Full:
            with self.changed:
                self.pending[service] -= 1
            raise RuntimeError("shared inference queue is full")
        return job

    def cancel(self, job):
        job.cancelled = True

    def finish(self, job):
        job.finished.set()

    def select(self, service):
        if service not in self.profiles:
            raise ValueError("unsupported RackAI service")
        with self.changed:
            old_service = self.state.reservation_service or self.selected
            retry = service == self.selected and self.state.handoff_state == HandoffState.FAILED
            if service == self.selected and not retry:
                return self.status()
            self.state.previous_service = old_service
            self.state.selected_service = service
            self.switching = True
            self._transition(HandoffState.RELEASING if self.state.reservation_id else HandoffState.RESERVING)
        try:
            if not self._wait_drained(old_service):
                return self._fail("drain_previous", "admitted work for previous service did not drain")
            with self.activation_lock:
                if not self._release_previous(old_service):
                    return self.status()
                try:
                    self._ensure_gateway(service)
                except Exception as exc:
                    phase = (
                        "readiness"
                        if self.state.handoff_state == HandoffState.PREPARING
                        else "reserve_new"
                    )
                    self._fail(phase, str(exc))
            return self.status()
        finally:
            with self.changed:
                self.switching = False
                self.changed.notify_all()

    def _wait_drained(self, service):
        deadline = time.monotonic() + PREPARE_TIMEOUT_SECONDS
        with self.changed:
            while self.pending[service] > 0 and time.monotonic() < deadline:
                self.changed.wait(min(1, deadline - time.monotonic()))
            return self.pending[service] == 0

    def _release_previous(self, service):
        with self.lock:
            reservation_id = (
                self.state.reservation_id if self.state.reservation_service == service else None
            )
        if not reservation_id:
            self._transition(HandoffState.RESERVING)
            return True
        try:
            result = self.call({"operation": "release_reservation", "reservation_id": reservation_id})
            self._poll_terminal(reservation_id, result)
        except Exception as exc:
            self._fail("release_previous", str(exc))
            return False
        with self.lock:
            self.state.reservation_id = None
            self.state.reservation_service = None
            self._transition(HandoffState.RESERVING)
        return True

    def _poll_terminal(self, reservation_id, result):
        deadline = time.monotonic() + PREPARE_TIMEOUT_SECONDS
        while result.get("state") not in TERMINAL:
            if time.monotonic() >= deadline:
                raise RuntimeError("previous RackAI reservation release timed out")
            time.sleep(1)
            result = self.call({"operation": "inspect_reservation", "reservation_id": reservation_id})

    def _ensure_gateway(self, service):
        with self.lock:
            reservation_id = (
                self.state.reservation_id if self.state.reservation_service == service else None
            )
        if reservation_id:
            result = self.call({"operation": "inspect_reservation", "reservation_id": reservation_id})
        else:
            self._transition(HandoffState.RESERVING)
            result = self.call({
                "operation": "reserve",
                "request": {
                    "acquisition_id": uuid.uuid4().hex,
                    "work_id": "shared-" + uuid.uuid4().hex,
                    "services": [service],
                    "priority": "medium",
                    "ttl_seconds": 1800,
                },
            })
            reservation_id = result.get("id")
            member = result.get("services", {}).get(service, {})
            if not isinstance(reservation_id, str) or self._terminal(result, member):
                raise RuntimeError(self._reason(result, member))
            with self.lock:
                self.state.reservation_id = reservation_id
                self.state.reservation_service = service
                self._transition(HandoffState.PREPARING)
        return self._poll_ready(service, reservation_id, result)

    def _poll_ready(self, service, reservation_id, result):
        deadline = time.monotonic() + PREPARE_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            member = result.get("services", {}).get(service, {})
            if (
                result.get("state") == "ready"
                and member.get("state") == "ready"
                and member.get("gateway_path")
            ):
                self._transition(HandoffState.READY)
                return member["gateway_path"]
            if self._terminal(result, member):
                with self.lock:
                    self.state.reservation_id = None
                    self.state.reservation_service = None
                raise RuntimeError(self._reason(result, member))
            self._transition(HandoffState.PREPARING)
            time.sleep(1)
            result = self.call({"operation": "inspect_reservation", "reservation_id": reservation_id})
        raise RuntimeError("RackAI reservation preparation timed out")

    @staticmethod
    def _terminal(result, member):
        return result.get("state") in TERMINAL or member.get("state") in TERMINAL

    @staticmethod
    def _reason(result, member):
        reason = member.get("reason") or member.get("why_not_ready") or result.get("reason") or "unspecified"
        return (
            "selected RackAI service unavailable "
            f"(state={result.get('state')}, member={member.get('state')}, reason={reason})"
        )

    def _transition(self, state):
        with self.changed:
            self.state.handoff_state = state
            if state != HandoffState.FAILED:
                self.state.failure_phase = None
                self.state.failure_reason = None
            self.store.write(self.state)
            self.changed.notify_all()

    def _fail(self, phase, reason):
        with self.changed:
            self.state.handoff_state = HandoffState.FAILED
            self.state.failure_phase = phase
            self.state.failure_reason = reason
            self.store.write(self.state)
            self.changed.notify_all()
            return self.status()

    def _restore(self):
        payload = self.store.read()
        selected = payload.get("selected_service")
        if selected in self.profiles:
            self.state.selected_service = selected
        self.state.previous_service = payload.get("previous_service")
        self.state.failure_phase = payload.get("failure_phase")
        self.state.failure_reason = payload.get("failure_reason")
        try:
            self.state.handoff_state = HandoffState(payload.get("handoff_state", "idle"))
        except ValueError:
            self.state.handoff_state = HandoffState.FAILED
        reservation_id = payload.get("reservation_id")
        service = payload.get("reservation_service")
        if not isinstance(reservation_id, str) or service != self.selected:
            self.state.reservation_id = None
            self.state.reservation_service = None
            self.store.write(self.state)
            return
        try:
            result = self.call({"operation": "inspect_reservation", "reservation_id": reservation_id})
            member = result.get("services", {}).get(service, {})
        except Exception as exc:
            self._fail("restart_recovery", str(exc))
            return
        if self._terminal(result, member):
            self.state.reservation_id = None
            self.state.reservation_service = None
            self._fail("restart_recovery", self._reason(result, member))
            return
        self.state.reservation_id = reservation_id
        self.state.reservation_service = service
        ready = result.get("state") == "ready" and member.get("state") == "ready"
        self.state.handoff_state = HandoffState.READY if ready else HandoffState.PREPARING
        self.store.write(self.state)

    def _validate(self, service, body):
        payload = json.loads(body)
        if payload.get("model") != ALIAS:
            raise ValueError(f"model must be {ALIAS}")
        requested = payload.get("max_tokens", payload.get("max_completion_tokens", 0)) or 0
        limit = self.profiles[service]["max_output_tokens"]
        if not isinstance(requested, int) or requested < 0 or requested > limit:
            raise ValueError("requested output exceeds selected RackAI service limit")

    def _run(self):
        while True:
            job = self.jobs.get()
            with self.changed:
                self.active = job
            try:
                if job.cancelled:
                    job.error = "request cancelled before dispatch"
                elif self._selection_failed(job.service):
                    job.error = self.state.failure_reason or "selected service handoff failed"
                else:
                    self._wait_handoff(job.service)
                    if self._selection_failed(job.service):
                        job.error = self.state.failure_reason or "selected service handoff failed"
                    else:
                        with self.activation_lock:
                            job.result = self._ensure_gateway(job.service)
            except Exception as exc:
                job.error = str(exc)
                if job.service == self.selected:
                    self._fail(self.state.failure_phase or "readiness", job.error)
            finally:
                job.done.set()
                job.finished.wait()
                with self.changed:
                    self.active = None
                    self.pending[job.service] -= 1
                    self.changed.notify_all()
                self.jobs.task_done()

    def _selection_failed(self, service):
        with self.lock:
            return service == self.selected and self.state.handoff_state == HandoffState.FAILED

    def _wait_handoff(self, service):
        with self.changed:
            while (
                service == self.selected
                and self.switching
            ):
                self.changed.wait(1)

