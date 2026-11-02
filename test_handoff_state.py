import tempfile
import threading
import time
from pathlib import Path

import shared_bridge_runtime as runtime_module
from bridge_test_support import FakeRuntime, PROFILES, request
from shared_bridge_runtime import Bridge, StateStore


def bridge_for(runtime):
    return Bridge(runtime, PROFILES, StateStore(Path(tempfile.mkdtemp()) / "state.json"))


# An active and then queued old-service request must both finish before release.
runtime = FakeRuntime()
bridge = bridge_for(runtime)
first = bridge.submit("one", "chat/completions", request("one"))
assert first.done.wait(2)
second = bridge.submit("two", "chat/completions", request("two"))
result = {}
switch = threading.Thread(target=lambda: result.update(bridge.select("maxi-brain")))
switch.start()
time.sleep(0.1)
assert switch.is_alive()
assert not any(call["operation"] == "release_reservation" for call in runtime.calls)
bridge.finish(first)
assert second.done.wait(2)
time.sleep(0.1)
assert switch.is_alive()
bridge.finish(second)
switch.join(3)
assert not switch.is_alive()
assert result["handoff_state"] == "ready"
assert result["selected_service"] == "maxi-brain"


class ReleaseFailure(FakeRuntime):
    def __call__(self, payload):
        if payload["operation"] == "release_reservation":
            self.calls.append(payload)
            raise RuntimeError("bounded release failure")
        return super().__call__(payload)


release_runtime = ReleaseFailure()
release_bridge = bridge_for(release_runtime)
job = release_bridge.submit("one", "chat/completions", request())
assert job.done.wait(2)
release_bridge.finish(job)
failed = release_bridge.select("maxi-brain")
assert failed["selected_service"] == "maxi-brain"
assert failed["previous_service"] == "mini-brain"
assert failed["handoff_state"] == "failed"
assert failed["failure_phase"] == "release_previous"
assert "bounded release failure" in failed["failure_reason"]
assert failed["reservation_service"] == "mini-brain"


class ReserveFailure(FakeRuntime):
    def __call__(self, payload):
        if payload["operation"] == "reserve":
            self.calls.append(payload)
            raise RuntimeError("capacity unavailable")
        return super().__call__(payload)


reserve_bridge = bridge_for(ReserveFailure())
failed = reserve_bridge.select("maxi-brain")
assert failed["selected_service"] == "maxi-brain"
assert failed["handoff_state"] == "failed"
assert failed["failure_phase"] == "reserve_new"
assert failed["failure_reason"] == "capacity unavailable"


class NeverReady(FakeRuntime):
    def __call__(self, payload):
        if payload["operation"] == "inspect_reservation":
            self.calls.append(payload)
            return self._view(payload["reservation_id"])
        return super().__call__(payload)


old_timeout = runtime_module.PREPARE_TIMEOUT_SECONDS
runtime_module.PREPARE_TIMEOUT_SECONDS = 0.05
try:
    preparing_bridge = bridge_for(NeverReady(preparing=True))
    failed = preparing_bridge.select("maxi-brain")
    assert failed["selected_service"] == "maxi-brain"
    assert failed["handoff_state"] == "failed"
    assert failed["failure_phase"] == "readiness"
    assert "timed out" in failed["failure_reason"]
finally:
    runtime_module.PREPARE_TIMEOUT_SECONDS = old_timeout

