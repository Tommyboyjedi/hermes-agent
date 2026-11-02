"""Loopback-only OpenAI-compatible gateway for one shared RackAI reservation."""
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from shared_bridge_runtime import ALIAS, Bridge, StateStore, discover_profiles

RUNTIME = os.environ.get("SHARED_RACKAI_RUNTIME", "http://127.0.0.1:8095/runtime/v1")
HOST = os.environ.get("SHARED_RACKAI_HOST", "127.0.0.1")
PORT = int(os.environ.get("SHARED_RACKAI_PORT", "18098"))
TOKEN = Path(os.environ.get(
    "SHARED_RACKAI_TOKEN_FILE",
    "/srv/rack-ai/deployments/idle-runtime/secrets/hermes",
))
BRIDGE_STATE_FILE = Path(
    os.environ.get(
        "SHARED_RACKAI_STATE_FILE",
        "/home/tomp/.local/share/rackai-shared-bridge/state.json",
    )
)


class RackAIError(RuntimeError):
    pass


def runtime_call(payload):
    token = TOKEN.read_text(encoding="utf-8").strip()
    request = Request(
        RUNTIME,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=20) as response:
            return json.load(response)["result"]
    except HTTPError as exc:
        try:
            body = json.loads(exc.read())
            detail = body.get("detail", {})
            reason = detail.get("reason") or body.get("error") or str(exc)
        except (ValueError, OSError):
            reason = str(exc)
        raise RackAIError(reason) from exc


def make_bridge(call=runtime_call, state_file=None):
    profiles = discover_profiles(call)
    return Bridge(call, profiles, StateStore(state_file or BRIDGE_STATE_FILE))


class HttpGateway(BaseHTTPRequestHandler):
    bridge = None
    upstream = staticmethod(urlopen)

    def log_message(self, *_args):
        pass

    def _json(self, status, value):
        data = json.dumps(value).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path in ("/health", "/healthz"):
            state = self.bridge.status()
            return self._json(200, {
                "status": "ok",
                "selected_service": state["selected_service"],
                "handoff_state": state["handoff_state"],
            })
        if self.path == "/v1/models":
            return self._json(200, {
                "object": "list",
                "data": [{"id": ALIAS, "object": "model", "owned_by": "rackai"}],
            })
        if self.path == "/control/status":
            if not self._authorized():
                return self._json(401, {"error": {"message": "unauthorized"}})
            return self._json(200, self.bridge.status())
        return self._json(404, {"error": {"message": "not found", "type": "not_found"}})

    def do_POST(self):
        if self.path == "/control/select":
            return self._select()
        if self.path != "/v1/chat/completions":
            return self._json(404, {"error": {"message": "not found", "type": "not_found"}})
        try:
            body = self._body()
            payload = json.loads(body)
            job = self.bridge.submit(
                self.headers.get("X-RackAI-Client", "unknown"),
                "chat/completions",
                body,
            )
            if not job.done.wait(125):
                self.bridge.cancel(job)
                raise RuntimeError("queue wait exceeded")
            if job.error:
                raise RuntimeError(job.error)
            self._forward(job, bool(payload.get("stream")))
        except (ValueError, json.JSONDecodeError) as exc:
            self._json(400, {"error": {"message": str(exc), "type": "invalid_request_error"}})
        except RuntimeError as exc:
            state = self.bridge.status()
            self._json(503, {
                "error": {
                    "message": str(exc),
                    "type": "service_unavailable",
                    "service": state["selected_service"],
                    "phase": state["failure_phase"],
                    "handoff_state": state["handoff_state"],
                }
            })
        finally:
            if "job" in locals():
                self.bridge.finish(job)

    def _select(self):
        if not self._authorized():
            return self._json(401, {"error": {"message": "unauthorized"}})
        try:
            payload = json.loads(self._body())
            return self._json(200, self.bridge.select(payload.get("service")))
        except (ValueError, json.JSONDecodeError) as exc:
            return self._json(400, {"error": {"message": str(exc)}})

    def _authorized(self):
        return self.headers.get("X-RackAI-Control") == TOKEN.read_text(encoding="utf-8").strip()

    def _body(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            raise ValueError("invalid request size")
        return self.rfile.read(length)

    def _forward(self, job, stream):
        gateway = job.result
        request = Request(
            "http://127.0.0.1:8095" + gateway.rstrip("/") + "/chat/completions",
            data=job.body,
            headers={
                "Authorization": "Bearer " + TOKEN.read_text(encoding="utf-8").strip(),
                "Content-Type": "application/json",
                "Accept": "text/event-stream" if stream else "application/json",
                "Idempotency-Key": job.request_id,
            },
            method="POST",
        )
        try:
            with self.upstream(request, timeout=None) as upstream:
                self._copy_upstream(upstream, stream)
        except HTTPError as exc:
            self._copy_upstream(exc, False)
        except (BrokenPipeError, ConnectionResetError):
            return
        except (URLError, OSError) as exc:
            raise RuntimeError("RackAI gateway transport failed") from exc

    def _copy_upstream(self, upstream, stream):
        self.send_response(upstream.status)
        self.send_header("Content-Type", upstream.headers.get("Content-Type", "application/json"))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        while True:
            chunk = upstream.read(4096)
            if not chunk:
                break
            self.wfile.write(chunk)
            if stream:
                self.wfile.flush()


def make_server(bridge, host="127.0.0.1", port=0):
    HttpGateway.bridge = bridge
    return ThreadingHTTPServer((host, port), HttpGateway)


if __name__ == "__main__":
    make_server(make_bridge(), HOST, PORT).serve_forever()

