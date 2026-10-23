#!/usr/bin/env python3
"""Test-owned process tree. The parent cooperates with INT across sessions.

The escaped child is deliberately outside Forcola's process group. In stubborn
mode it ignores INT/TERM and must be cleaned up by the test, never by Forcola.
"""
import os
import signal
import sys
import time

mode, directory = sys.argv[1:]


def record(name, value):
    with open(os.path.join(directory, name), "a", encoding="utf-8") as stream:
        stream.write(str(value) + "\n")


child = os.fork()
if child == 0:
    os.setsid()
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN if mode in ("stubborn", "root_exit") else lambda *_: sys.exit(0))
    record("child", os.getpid())
    while True:
        signal.pause()


def interrupt(*_):
    record("signals", "INT")
    if mode == "cooperate":
        os.kill(child, signal.SIGINT)
        os.waitpid(child, 0)
        sys.exit(42)


def terminate(*_):
    record("signals", "TERM")


signal.signal(signal.SIGINT, interrupt)
signal.signal(signal.SIGTERM, terminate)
# Ready file is written only after both handlers and the escaped child exist.
while not os.path.exists(os.path.join(directory, "child")):
    time.sleep(0.005)
sys.stdout.write('{"type":"result","ok":true}\n')
sys.stdout.flush()
record("parent", os.getpid())
if mode == "root_exit":
    sys.exit(0)
while True:
    signal.pause()
