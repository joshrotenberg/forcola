#!/usr/bin/env python3
"""TRACKED compatibility and evidence fixture. Never starts a child."""
import json
import os
import struct
import sys

MODE = "valid"
DIRECTORY = os.path.dirname(__file__)
ROOT = {"pid": 123, "pgid": 123, "start_time": {"kind": "macos", "seconds": 100, "microseconds": 7}}
CHILD = {"pid": 456, "pgid": 456, "start_time": {"kind": "macos", "seconds": 101, "microseconds": 8}}


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


selected = []
request = read_frame()
if request and request[0] == 7:
    with open(os.path.join(DIRECTORY, "hello"), "wb") as out:
        out.write(request[1:])
    selected = json.loads(request[1:]).get("features", []) if request[1:] else []
    features = ["bounded_run_v1", "env_unset_v1", "spawn_identity_v1", "structured_spawn_errors_v1"]
    if MODE != "no_tracking":
        features.append("track_descendants_v1")
    frame(0x15, {"protocol": 1, "features": features})
    request = read_frame()
if request is None:
    sys.exit(0)
if request[0] != 1:
    sys.exit(2)
with open(os.path.join(DIRECTORY, "spawn"), "wb") as out:
    out.write(request[1:])
if "spawn_identity_v1" in selected:
    frame(0x16, ROOT)
if "track_descendants_v1" in selected and MODE != "missing_root":
    frame(0x17, ROOT)
    frame(0x17, CHILD)
    frame(0x17, CHILD)
    if MODE == "malformed":
        frame(0x17, b"{broken")
    if MODE == "not_leader":
        frame(0x17, {**CHILD, "pgid": 789})
    if MODE == "overflow":
        for pid in range(1000, 5097):
            frame(0x17, {**CHILD, "pid": pid, "pgid": pid})
frame(0x11, b"ok\n")
report = {"status": 0, "confirmed": True, "timed_out": False, "contained": False, "output_truncated": False}
if "track_descendants_v1" in selected:
    if MODE == "unknown_scope":
        report["scope"] = "invented_scope"
    elif MODE == "group_scope":
        report["scope"] = "process_group"
    elif MODE != "missing_scope":
        report["scope"] = "tree_sampled"
frame(0x13, report)
while read_frame() is not None:
    pass
