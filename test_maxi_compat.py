import json
import tempfile
from pathlib import Path

from bridge_test_support import FakeRuntime, PROFILES, request
from shared_bridge_runtime import Bridge, StateStore

runtime = FakeRuntime()
bridge = Bridge(runtime, PROFILES, StateStore(Path(tempfile.mkdtemp()) / "state.json"))
bridge.select("maxi-brain")
body = request("same logical request")
job = bridge.submit("opencode", "chat/completions", body)
assert job.done.wait(2)
forwarded = json.loads(job.body)
bridge.finish(job)
assert forwarded["model"] == "rackai-service"
assert "chat_template_kwargs" not in forwarded
assert "reasoning_effort" not in forwarded
assert "input_token_preflight" not in PROFILES["maxi-brain"]
assert "backend" not in PROFILES["maxi-brain"]

