#!/usr/bin/env python3
"""Send tracking metadata without output. Never starts a child."""
import json
import os
import select
import struct
import sys
import time


def read_exact(size):
    data = b""
    while len(data) < size:
        chunk = os.read(0, size - len(data))
        if not chunk:
            return None
        data += chunk
    return data


def read_frame():
    length = read_exact(4)
    if length is None:
        return None
    return read_exact(struct.unpack(">I", length)[0])


def frame(tag, payload):
    payload = json.dumps(payload).encode()
    sys.stdout.buffer.write(struct.pack(">I", len(payload) + 1) + bytes([tag]) + payload)
    sys.stdout.buffer.flush()


request = read_frame()
if request is None or request[0] != 0x07:
    sys.exit(1)
frame(0x15, {"protocol": 1, "features": ["track_descendants_v1"]})
request = read_frame()
if request is None or request[0] != 0x01:
    sys.exit(1)

deadline = time.monotonic() + 1.0
next_observation = time.monotonic()
pid = 100
while True:
    now = time.monotonic()
    if now >= next_observation:
        frame(0x17, {"pid": pid, "pgid": pid,
                     "start_time": {"kind": "macos", "seconds": pid, "microseconds": 0}})
        pid += 1
        next_observation = now + 0.02
    if now >= deadline:
        break
    readable, _, _ = select.select([sys.stdin], [], [], 0.01)
    if readable:
        request = read_frame()
        if request is None:
            sys.exit(0)
        if request[0] == 0x04:
            break

frame(0x13, {"status": 0, "confirmed": True, "scope": "tree_sampled",
             "timed_out": False, "contained": False, "output_truncated": False})
while read_frame() is not None:
    pass
