from bridge_test_support import PROFILES
from shared_bridge_runtime import CONTRACT, discover_profiles

profiles = discover_profiles(lambda payload: {"tags": list(PROFILES.values())})
assert set(profiles) == set(PROFILES)
for profile in profiles.values():
    assert profile["inference_service"]["contract"] == CONTRACT
    assert "backend" not in profile
    assert "driver" not in profile
    assert "endpoint" not in profile
    assert "model" not in profile

invalid = dict(PROFILES["mini-brain"])
invalid["inference_service"] = {"contract": "old"}
try:
    discover_profiles(lambda payload: {"tags": [invalid]})
except RuntimeError:
    pass
else:
    raise AssertionError("invalid service contract accepted")

