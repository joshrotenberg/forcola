#!/usr/bin/env python3
"""Capability fixture. Records frames without starting a child."""
import json
import os
import struct
import sys
import time

MODE = "all"
DIRECTORY = os.path.dirname(__file__)


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


request = read_frame()
if request and request[0] == 7:
    with open(os.path.join(DIRECTORY, "hello"), "wb") as out:
        out.write(request[1:])
    if MODE == "stall":
        read_frame()
        sys.exit(0)
    features = ["spawn_identity_v1", "structured_spawn_errors_v1"]
    if MODE != "no_bounded":
        features.append("bounded_run_v1")
    if MODE != "no_unset":
        features.append("env_unset_v1")
    if MODE == "delay":
        time.sleep(0.15)
    frame(0x15, {"protocol": 1, "features": features})
    request = read_frame()
if request and request[0] == 1:
    with open(os.path.join(DIRECTORY, "spawn"), "wb") as out:
        out.write(request[1:])
    frame(0x11, b"ok\n")
    frame(0x13, {"status": 0, "confirmed": True, "timed_out": False})
while read_frame() is not None:
    pass
