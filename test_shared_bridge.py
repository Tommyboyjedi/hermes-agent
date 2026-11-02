import tempfile
from pathlib import Path

from bridge_test_support import FakeRuntime, PROFILES, request
from shared_bridge_runtime import Bridge, StateStore

runtime = FakeRuntime()
bridge = Bridge(runtime, PROFILES, StateStore(Path(tempfile.mkdtemp()) / "state.json"))
first = bridge.submit("hermes", "chat/completions", request("one"))
assert first.service == "mini-brain"
assert first.done.wait(2) and first.result.endswith("/v1")
bridge.finish(first)
status = bridge.select("local-coder")
assert status["selected_service"] == "local-coder"
assert status["handoff_state"] == "ready"
second = bridge.submit("opencode", "chat/completions", request("two"))
assert second.service == "local-coder"
assert second.done.wait(2) and second.result.endswith("/v1")
bridge.finish(second)
assert len([call for call in runtime.calls if call["operation"] == "reserve"]) == 2
try:
    bridge.submit("opencode", "chat/completions", request(max_tokens=99999))
except ValueError:
    pass
else:
    raise AssertionError("output limit accepted")

