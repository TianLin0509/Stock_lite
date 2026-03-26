from __future__ import annotations

import argparse
from pathlib import Path

import paramiko


def mkdir_p(sftp: paramiko.SFTPClient, remote_dir: str) -> None:
    parts = remote_dir.replace("\\", "/").split("/")
    current = parts[0]
    if current.endswith(":"):
        current += "/"
    for part in parts[1:]:
        if not part:
            continue
        if not current.endswith("/"):
            current += "/"
        current += part
        try:
            sftp.stat(current)
        except OSError:
            sftp.mkdir(current)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--key", required=True)
    parser.add_argument("--local", required=True)
    parser.add_argument("--remote", required=True)
    args = parser.parse_args()

    key_path = Path(args.key)
    pkey = paramiko.Ed25519Key.from_private_key_file(str(key_path))

    transport = paramiko.Transport((args.host, 22))
    transport.connect(username=args.user, pkey=pkey)
    sftp = paramiko.SFTPClient.from_transport(transport)
    try:
        remote_path = args.remote.replace("\\", "/")
        remote_dir = remote_path.rsplit("/", 1)[0]
        mkdir_p(sftp, remote_dir)
        sftp.put(args.local, remote_path)
        print(remote_path)
        return 0
    finally:
        sftp.close()
        transport.close()


if __name__ == "__main__":
    raise SystemExit(main())
