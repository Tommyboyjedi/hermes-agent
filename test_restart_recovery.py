import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from bridge_test_support import FakeRuntime, PROFILES
from shared_bridge_runtime import Bridge, HandoffState, StateStore

root = Path(tempfile.mkdtemp())
state_file = root / "state.json"
runtime = FakeRuntime()
runtime.reservations["persisted"] = ("mini-brain", "ready")
first = Bridge(runtime, PROFILES, StateStore(state_file))
first.reservation = {"id": "persisted", "service": "mini-brain"}
first.state.handoff_state = HandoffState.READY
first.store.write(first.state)
restored = Bridge(runtime, PROFILES, StateStore(state_file))
assert restored.selected == "mini-brain"
assert restored.reservation["id"] == "persisted"
assert restored.status()["handoff_state"] == "ready"

state_file.write_text(json.dumps({
    "selected_service": "maxi-brain",
    "previous_service": "mini-brain",
    "handoff_state": "failed",
    "failure_phase": "activation",
    "failure_reason": "backend failed",
    "reservation_id": None,
    "reservation_service": None,
}))
failed = Bridge(runtime, PROFILES, StateStore(state_file))
status = failed.status()
assert status["selected_service"] == "maxi-brain"
assert status["previous_service"] == "mini-brain"
assert status["handoff_state"] == "failed"
assert status["failure_reason"] == "backend failed"

class RuntimeHandler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        assert body["operation"] == "discover"
        data = json.dumps({"schema": "rack-ai/runtime/v1", "result": {"tags": list(PROFILES.values())}}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

mock = ThreadingHTTPServer(("127.0.0.1", 0), RuntimeHandler)
threading.Thread(target=mock.serve_forever, daemon=True).start()
token = root / "token"
token.write_text("test")
port = 18199
env = os.environ | {
    "SHARED_RACKAI_PORT": str(port),
    "SHARED_RACKAI_STATE_FILE": str(root / "server.json"),
    "SHARED_RACKAI_RUNTIME": f"http://127.0.0.1:{mock.server_port}/runtime/v1",
    "SHARED_RACKAI_TOKEN_FILE": str(token),
}
process = subprocess.Popen([sys.executable, "shared_bridge.py"], env=env)
try:
    deadline = time.time() + 5
    while time.time() < deadline:
        with socket.socket() as sock:
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                break
        time.sleep(0.05)
    else:
        raise AssertionError("server executable did not listen")
finally:
    process.terminate()
    process.wait(5)
    mock.shutdown()

