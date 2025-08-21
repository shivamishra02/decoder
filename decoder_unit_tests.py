
import json, sys
from math import isclose

# import local sample file
SAMPLE = "sample_zeru.json"


def hex_to_int(h):
    if not h or h == "0x": return 0
    return int(h,16)

def int256_from_hexslot(slot_hex):
    val = int(slot_hex, 16)
    if val >= 2**255:
        val = val - 2**256
    return val

# Basic tests
def test_hex_to_int():
    assert hex_to_int("0x0") == 0
    assert hex_to_int("0x10") == 16
    assert hex_to_int("0x0000000000000000000000000000000000000005") == 5

def test_int256_neg():
    # two's complement: -1 is all ff..ff (256)
    allf = "f"*64
    assert int256_from_hexslot(allf) == -1

def test_sample_load_and_transfer_detection():
    data = json.load(open(SAMPLE))
    logs = data.get("logs", [])
    # find a transfer topic signature
    found = False
    for l in logs:
        if l.get("topics") and len(l["topics"])>0 and l["topics"][0].startswith("0xddf252ad"):
            found = True
            # check that data is numeric
            v = hex_to_int(l.get("data","0x0"))
            assert isinstance(v, int)
            break
    assert found, "No Transfer event found in sample"

def run_all():
    test_hex_to_int()
    test_int256_neg()
    test_sample_load_and_transfer_detection()
    print("ALL TESTS PASSED")

if __name__ == "__main__":
    run_all()
