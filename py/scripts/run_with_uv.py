from __future__ import annotations

import os
import subprocess
import sys


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: run_with_uv.py <command> [args...]", file=sys.stderr)
        return 2

    env = os.environ.copy()
    env.pop("VIRTUAL_ENV", None)

    command = ["uv", "run", "--no-sync", *sys.argv[1:]]
    return subprocess.call(command, env=env)


if __name__ == "__main__":
    raise SystemExit(main())