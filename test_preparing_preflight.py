import tempfile
from pathlib import Path

from bridge_test_support import FakeRuntime, PROFILES
from shared_bridge_runtime import Bridge, StateStore

runtime = FakeRuntime(preparing=True)
bridge = Bridge(runtime, PROFILES, StateStore(Path(tempfile.mkdtemp()) / "state.json"))
gateway = bridge._ensure_gateway("mini-brain")
assert gateway.endswith("/v1")
assert bridge.reservation["id"] == "r-mini-brain"
assert len([call for call in runtime.calls if call["operation"] == "reserve"]) == 1
assert runtime.inspect_count == 1

