#!/usr/bin/env python3
"""Protocol compatibility and shutdown fixture. Never starts a child."""
import json
import os
import struct
import sys

MODE = "legacy"


def read_frame():
    length = sys.stdin.buffer.read(4)
    if len(length) != 4:
        return None
    return sys.stdin.buffer.read(struct.unpack(">I", length)[0])


def frame(tag, payload):
    if not isinstance(payload, bytes):
        payload = json.dumps(payload).encode()
    sys.stdout.buffer.write(struct.pack(">I", len(payload) + 1) + bytes([tag]) + payload)
    sys.stdout.buffer.flush()


first = read_frame()
if first is not None and first[0] == 7:
    if MODE == "legacy":
        frame(0x14, {"reason": "expected SPAWN"})
        sys.exit(0)
    features = ["bounded_run_v1"]
    if MODE == "late_spawned":
        features.append("spawn_identity_v1")
    frame(0x15, {"protocol": 1, "features": features})
    first = read_frame()
if first is None:
    sys.exit(0)
if first[0] != 1:
    sys.exit(2)
with open(os.path.join(os.path.dirname(__file__), "spawn_received"), "w") as marker:
    marker.write("SPAWN")
if MODE != "late_spawned":
    frame(0x11, b"legacy\n")
    frame(0x13, {"status": 0})
while True:
    request = read_frame()
    if request is None:
        break
    if request[0] == 4 and MODE == "late_spawned":
        frame(0x16, {"pid": 123, "pgid": 123,
                     "start_time": {"kind": "macos", "seconds": 1234, "microseconds": 5678}})
        frame(0x13, {"signal": 15, "confirmed": True, "timed_out": False})
