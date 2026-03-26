from __future__ import annotations

import argparse
import sys
from pathlib import Path

import paramiko


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--key", required=True)
    parser.add_argument("--command")
    parser.add_argument("--script-file")
    parser.add_argument("--timeout", type=int, default=3600)
    args = parser.parse_args()

    if bool(args.command) == bool(args.script_file):
        parser.error("provide exactly one of --command or --script-file")

    key_path = Path(args.key)
    pkey = paramiko.Ed25519Key.from_private_key_file(str(key_path))

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=args.host,
        username=args.user,
        pkey=pkey,
        timeout=20,
        banner_timeout=20,
        auth_timeout=20,
    )
    try:
        command = args.command
        if args.script_file:
            script_text = Path(args.script_file).read_text(encoding="utf-8")
            command = 'powershell -NoProfile -ExecutionPolicy Bypass -Command -'
        stdin, stdout, stderr = client.exec_command(command, timeout=args.timeout)
        if args.script_file:
            stdin.write(script_text)
            stdin.channel.shutdown_write()
        out = stdout.read().decode("utf-8", errors="ignore")
        err = stderr.read().decode("utf-8", errors="ignore")
        if out:
            sys.stdout.buffer.write(out.encode("utf-8", errors="ignore"))
        if err:
            sys.stderr.buffer.write(err.encode("utf-8", errors="ignore"))
        return stdout.channel.recv_exit_status()
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
