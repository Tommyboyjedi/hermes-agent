import importlib.util, json, tempfile
from pathlib import Path
from fastapi import HTTPException
spec=importlib.util.spec_from_file_location("router","hermes_cli/web_routers/rackai_service.py")
m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
m._TOKEN=Path(tempfile.mkstemp()[1]); m._TOKEN.write_text("secret")
seen=[]
class Response:
    def __enter__(self): return self
    def __exit__(self,*_): pass
    def read(self): return b'{"selected_service":"mini-brain","queue_length":0}'
def fake(req,timeout):
    seen.append((req.full_url,req.get_method(),req.headers.get("X-rackai-control"),timeout))
    return Response()
m.urlopen=fake
assert m._control("/control/status")["selected_service"]=="mini-brain"
assert m._control("/control/select",{"service":"mini-brain"})["queue_length"]==0
assert seen[0][1]=="GET" and seen[1][1]=="POST" and seen[0][2]=="secret"
assert seen[0][3]==2 and seen[1][3]==130

# Both routes must pass through Hermes's established dashboard guard before
# the loopback control API is contacted.
guarded=[]
m._require_token=lambda request: guarded.append(request)
m._control=lambda path,payload=None: {"path":path,"payload":payload}
request=object()
assert m.rackai_status(request)["path"]=="/control/status"
assert m.rackai_select(request,{"service":"maxi-brain"})["payload"]=={"service":"maxi-brain"}
assert guarded==[request,request]

def denied(_request):
    raise HTTPException(status_code=401,detail="unauthorized")
m._require_token=denied
try:
    m.rackai_status(request)
    raise AssertionError("anonymous status request was accepted")
except HTTPException as exc:
    assert exc.status_code==401
try:
    m.rackai_select(request,{"service":"maxi-brain"})
    raise AssertionError("anonymous selection request was accepted")
except HTTPException as exc:
    assert exc.status_code==401
