import json
import threading
import time

from shared_bridge_runtime import ALIAS, CONTRACT


def descriptor(tag, max_input=1000, max_output=32):
    return {
        "tag": tag,
        "version": "test-v1",
        "qualified": True,
        "capabilities": ["reasoning", "coding"],
        "context_tokens": max_input + max_output,
        "max_input_tokens": max_input,
        "max_output_tokens": max_output,
        "inference_service": {
            "contract": CONTRACT,
            "model_alias": ALIAS,
            "protocols": ["chat_completions"],
            "streaming": {"supported": True, "delivery": "buffered_sse"},
            "tools": {"execution": "client_owned"},
        },
    }


PROFILES = {
    "mini-brain": descriptor("mini-brain"),
    "maxi-brain": descriptor("maxi-brain", 650000, 250000),
    "dual-brain": descriptor("dual-brain"),
    "local-coder": descriptor("local-coder", 14320, 2048),
    "local-primary": descriptor("local-primary", 65536, 65536),
}


def request(content="hi", stream=False, max_tokens=1):
    return json.dumps({
        "model": ALIAS,
        "stream": stream,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": content}],
    }).encode()


class FakeRuntime:
    def __init__(self, profiles=None, preparing=False):
        self.profiles = profiles or PROFILES
        self.preparing = preparing
        self.calls = []
        self.reservations = {}
        self.inspect_count = 0
        self.lock = threading.Lock()

    def __call__(self, payload):
        with self.lock:
            self.calls.append(payload)
            operation = payload["operation"]
            if operation == "discover":
                return {"tags": list(self.profiles.values())}
            if operation == "reserve":
                service = payload["request"]["services"][0]
                reservation_id = "r-" + service
                state = "preparing" if self.preparing else "ready"
                self.reservations[reservation_id] = (service, state)
                return self._view(reservation_id)
            if operation == "inspect_reservation":
                reservation_id = payload["reservation_id"]
                self.inspect_count += 1
                if self.preparing and self.inspect_count >= 1:
                    service, _ = self.reservations[reservation_id]
                    self.reservations[reservation_id] = (service, "ready")
                return self._view(reservation_id)
            if operation == "release_reservation":
                reservation_id = payload["reservation_id"]
                service, _ = self.reservations[reservation_id]
                self.reservations[reservation_id] = (service, "released")
                return self._view(reservation_id)
            raise AssertionError(operation)

    def _view(self, reservation_id):
        service, state = self.reservations[reservation_id]
        member = {"state": state}
        if state == "ready":
            member["gateway_path"] = f"/scoped/{reservation_id}/cap/v1"
        return {
            "id": reservation_id,
            "state": state,
            "services": {service: member},
        }

