"""Authenticated dashboard routes for the staged local RackAI shared bridge."""
import json
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from fastapi import APIRouter, HTTPException, Request as FastAPIRequest
from hermes_cli.web_deps import late

router = APIRouter()
_require_token = late("_require_token")
_BRIDGE = "http://127.0.0.1:18098"
_TOKEN = Path("/srv/rack-ai/deployments/idle-runtime/secrets/hermes")

def _control(path, payload=None):
    try:
        token = _TOKEN.read_text(encoding="utf-8").strip()
        request = Request(_BRIDGE + path, data=None if payload is None else json.dumps(payload).encode("utf-8"), headers={"X-RackAI-Control": token, "Content-Type": "application/json"}, method="GET" if payload is None else "POST")
        # Status is expected to be immediate; selection may wait for admitted
        # work to drain and for RackAI's bounded service preparation.
        timeout = 2 if payload is None else 130
        with urlopen(request, timeout=timeout) as response:
            return json.load(response)
    except (OSError, URLError, HTTPError, ValueError) as exc:
        raise HTTPException(status_code=503, detail="shared RackAI bridge unavailable") from exc

@router.get("/api/rackai-service/status")
def rackai_status(request: FastAPIRequest):
    _require_token(request)
    return _control("/control/status")

@router.post("/api/rackai-service/select")
def rackai_select(request: FastAPIRequest, payload: dict):
    _require_token(request)
    service = payload.get("service")
    if not isinstance(service, str):
        raise HTTPException(status_code=400, detail="service is required")
    return _control("/control/select", {"service": service})
