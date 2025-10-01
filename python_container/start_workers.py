#!/usr/bin/env python3
"""Bootstrap script that can run the DB writer, crawler ingest worker, or both."""

import os
import signal
import subprocess
import sys
import time
from typing import Dict

SCRIPT_MAP = {
    "db_writer": "db_writer.py",
    "db": "db_writer.py",
    "writer": "db_writer.py",
    "crawler_ingest": "crawler_ingest.py",
    "crawler": "crawler_ingest.py",
    "ingest": "crawler_ingest.py",
    "both": None,
    "all": None,
}


def run_single(script: str) -> int:
    return subprocess.call([sys.executable, script])


def run_all() -> int:
    processes: Dict[str, subprocess.Popen] = {}

    def stop_all(signum=None, frame=None) -> None:  # noqa: ARG001 - required by signal handler
        for proc in processes.values():
            if proc.poll() is None:
                proc.terminate()
        for proc in processes.values():
            if proc.poll() is None:
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, stop_all)

    processes["crawler_ingest"] = subprocess.Popen([sys.executable, "crawler_ingest.py"])
    processes["db_writer"] = subprocess.Popen([sys.executable, "db_writer.py"])

    exit_code = 0
    try:
        while processes:
            time.sleep(1)
            for name, proc in list(processes.items()):
                ret = proc.poll()
                if ret is not None:
                    exit_code = ret
                    processes.pop(name)
                    stop_all()
                    return exit_code
    finally:
        stop_all()
    return exit_code


def main() -> None:
    mode = os.getenv("WORKER_MODE", "both").strip().lower()
    target = SCRIPT_MAP.get(mode, None)

    if mode in {"both", "all"}:
        sys.exit(run_all())

    if target is None:
        print(
            f"Unsupported WORKER_MODE='{mode}'."
            " Valid options: db_writer, crawler_ingest, both",
            file=sys.stderr,
        )
        sys.exit(2)

    sys.exit(run_single(target))


if __name__ == "__main__":
    main()
