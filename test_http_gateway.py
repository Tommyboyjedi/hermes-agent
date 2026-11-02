import json
import tempfile
import threading
from pathlib import Path
from urllib.request import Request, urlopen

import shared_bridge as gateway
from bridge_test_support import FakeRuntime, PROFILES, request
from shared_bridge_runtime import Bridge, StateStore

token = Path(tempfile.mkstemp()[1])
token.write_text("test")
gateway.TOKEN = token
runtime = FakeRuntime()

class Upstream:
    status = 200
    headers = {"Content-Type": "text/event-stream"}
    def __init__(self):
        self.parts = iter([
            b'data: {"model":"rackai-service","choices":[{"delta":{"tool_calls":[{"id":"t"}]}}]}\n\n',
            b'data: [DONE]\n\n',
        ])
    def __enter__(self):
        return self
    def __exit__(self, *_):
        pass
    def read(self, _=None):
        return next(self.parts, b"")

forwarded = []
def upstream(req, *_args, **_kwargs):
    forwarded.append((json.loads(req.data), req.headers.get("Idempotency-key")))
    return Upstream()

gateway.HttpGateway.upstream = staticmethod(upstream)
bridge = Bridge(runtime, PROFILES, StateStore(Path(tempfile.mkdtemp()) / "state.json"))
server = gateway.make_server(bridge)
threading.Thread(target=server.serve_forever, daemon=True).start()
base = f"http://127.0.0.1:{server.server_port}"
models = json.load(urlopen(base + "/v1/models"))
assert models["data"][0]["id"] == "rackai-service"
with urlopen(Request(
    base + "/v1/chat/completions",
    data=request("hi", stream=True),
    headers={"X-RackAI-Client": "opencode", "Content-Type": "application/json"},
)) as response:
    data = response.read()
assert b"tool_calls" in data and data.endswith(b"data: [DONE]\n\n")
body, key = forwarded[0]
assert body["model"] == "rackai-service"
assert "chat_template_kwargs" not in body
assert "reasoning_effort" not in body
assert key
server.shutdown()

