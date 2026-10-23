#!/usr/bin/env python3
"""Small intentionally incomplete protocol peer; never executes a child."""
import json
import os
import signal
import struct
import sys
import time

MODE = "missing"


def read_frame():
    length = sys.stdin.buffer.read(4)
    if len(length) != 4:
        return None
    body = sys.stdin.buffer.read(struct.unpack(">I", length)[0])
    return body


def frame(tag, payload):
    if not isinstance(payload, bytes):
        payload = json.dumps(payload).encode()
    sys.stdout.buffer.write(struct.pack(">I", len(payload) + 1) + bytes([tag]) + payload)
    sys.stdout.buffer.flush()


directory = os.path.dirname(__file__)
with open(os.path.join(directory, "shim_opened"), "w") as marker:
    marker.write(MODE)
first = read_frame()
if MODE.startswith("handshake_"):
    if first is None or first[0] != 7:
        sys.exit(2)
    if MODE == "handshake_timeout":
        # Wait for owner EOF, not an arbitrary sleep or a child process.
        while read_frame() is not None:
            pass
        sys.exit(0)
    if MODE == "handshake_exit":
        sys.exit(23)
    if MODE == "handshake_crash":
        os.kill(os.getpid(), signal.SIGKILL)
    if MODE == "handshake_malformed":
        frame(0x15, b"{bad json")
    elif MODE == "handshake_bad_shape":
        frame(0x15, {"protocol": 1, "features": [42]})
    elif MODE == "handshake_wrong_frame":
        frame(0x11, b"unexpected output before SPAWN")
    elif MODE == "handshake_incompatible":
        frame(0x15, {"protocol": 2, "features": ["bounded_run_v1"]})
    elif MODE == "handshake_no_feature":
        frame(0x15, {"protocol": 1, "features": []})
    elif MODE in ("handshake_slow", "handshake_budget"):
        # Deterministic peer delay exercises the startup budget. No child runs.
        time.sleep(1.1 if MODE == "handshake_slow" else 0.4)
        frame(0x15, {"protocol": 1, "features": ["bounded_run_v1"]})
    first = read_frame()
if MODE == "legacy_cgroup":
    # Models an older peer that understands required cgroups but not HELLO.
    # This asserts wire/collector compatibility, not Linux containment.
    if first is None or first[0] != 1:
        frame(0x14, {"reason": "expected SPAWN"})
        sys.exit(0)
    with open(os.path.join(directory, "spawn_received"), "wb") as marker:
        marker.write(first[1:])
    frame(0x11, b"legacy output")
    frame(0x13, {"status": 0})
    while read_frame() is not None:
        pass
    sys.exit(0)
if MODE == "legacy":
    # Old shims reject HELLO. They must never receive SPAWN on a strict run.
    if first is not None and first[0] == 1:
        with open(os.path.join(directory, "spawn_received"), "w") as marker:
            marker.write("SPAWN")
    frame(0x14, {"reason": "expected SPAWN"})
    sys.exit(0)
if first is not None and first[0] == 7:
    frame(0x15, {"protocol": 1, "features": ["bounded_run_v1"]})
    first = read_frame()
if first is None or first[0] != 1:
    sys.exit(1)
with open(os.path.join(directory, "spawn_received"), "wb") as marker:
    marker.write(first[1:])
frame(0x11, b'{"type":"result","ok":true}\n')
if MODE == "missing":
    sys.exit(0)
elif MODE == "malformed":
    frame(0x13, b'{bad json')
elif MODE == "missing_confirmation":
    frame(0x13, {"status": 0})
elif MODE == "bad_confirmation":
    frame(0x13, {"status": 0, "confirmed": "yes", "contained": False})
elif MODE == "signal":
    frame(0x13, {"signal": 9, "confirmed": False, "contained": False,
                 "timed_out": False, "output_truncated": True})
elif MODE == "error":
    frame(0x14, {"reason": "fixture error"})
elif MODE == "handshake_slow":
    frame(0x13, {"status": 0, "confirmed": True, "contained": False})
# Keep the read side alive for racing EOF/credit, just like the real shim.
while True:
    request = read_frame()
    if request is None:
        break
    if request[0] == 4 and MODE == "drop_after_kill":
        break
    if request[0] == 4 and MODE == "malformed_after_kill":
        frame(0x13, b"{bad json")
    if request[0] == 4 and MODE == "handshake_budget":
        frame(0x13, {"status": 0, "confirmed": True, "contained": False})
