#!/usr/bin/env python3
"""Small framed-protocol fixture for terminal evidence; spawns no process."""

import json
import struct
import sys


def frame(tag, payload):
    packet = bytes([tag]) + payload
    sys.stdout.buffer.write(struct.pack(">I", len(packet)) + packet)
    sys.stdout.buffer.flush()


header = sys.stdin.buffer.read(4)
if len(header) != 4:
    raise SystemExit("missing spawn")
length = struct.unpack(">I", header)[0]
if length > 65536:
    raise SystemExit("oversize spawn")
packet = sys.stdin.buffer.read(length)
if len(packet) != length or packet[0] != 1:
    raise SystemExit("invalid spawn")

mode = json.loads(packet[1:])["argv"][0]
frame(0x11, b"ready\n")

while True:
    header = sys.stdin.buffer.read(4)
    if len(header) != 4:
        break
    length = struct.unpack(">I", header)[0]
    if length > 65536:
        raise SystemExit("oversize frame")
    packet = sys.stdin.buffer.read(length)
    if len(packet) != length:
        break
    if packet[0] == 4:
        if mode == "timeout":
            continue
        frame(
            0x13,
            json.dumps(
                {
                    "status": 7,
                    "confirmed": mode != "unconfirmed",
                    "contained": mode == "contained",
                }
            ).encode(),
        )
        break
