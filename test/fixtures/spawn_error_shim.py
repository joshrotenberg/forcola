#!/usr/bin/env python3
"""Structured error delivery fixture. It never executes a child."""
import json
import os
import struct
import sys

MODE = "cgroup_error"


def read_frame():
    size = sys.stdin.buffer.read(4)
    if len(size) != 4:
        return None
    return sys.stdin.buffer.read(struct.unpack(">I", size)[0])


def frame(tag, payload):
    if not isinstance(payload, bytes):
        payload = json.dumps(payload).encode()
    data = bytes([tag]) + payload
    sys.stdout.buffer.write(struct.pack(">I", len(data)) + data)
    sys.stdout.buffer.flush()


def error():
    if MODE == "malformed_details":
        frame(0x14, {"reason": "original spawn failure", "spawn_error": {}})
        return
    if MODE == "malformed_frame":
        frame(0x14, b"{broken")
        return
    if MODE == "invalid_reason":
        frame(0x14, {"reason": 42})
        return
    frame(0x14, {
        "reason": "spawn failed: Permission denied (os error 13)",
        "spawn_error": {
            "stage": "cgroup", "reason": "eacces", "errno": 13,
            "path": "/test-cgroup/cgroup.procs"
        }
    })


request = read_frame()
if request is not None and request[0] == 7:
    with open(os.path.join(os.path.dirname(__file__), "hello_received"), "wb") as out:
        out.write(request[1:])
    if MODE == "stall":
        read_frame()
        sys.exit(0)
    if MODE == "legacy":
        frame(0x14, {"reason": "expected SPAWN as the first frame"})
        sys.exit(0)
    features = ["bounded_run_v1", "spawn_identity_v1"]
    if MODE != "no_errors":
        features.append("structured_spawn_errors_v1")
    frame(0x15, {"protocol": 1, "features": features})
    request = read_frame()
if request is None:
    sys.exit(0)
if request[0] != 1:
    sys.exit(2)
with open(os.path.join(os.path.dirname(__file__), "spawn_received"), "w") as out:
    out.write("SPAWN")
if MODE != "late_error":
    error()
while True:
    request = read_frame()
    if request is None:
        break
    if request[0] == 4 and MODE == "late_error":
        error()
