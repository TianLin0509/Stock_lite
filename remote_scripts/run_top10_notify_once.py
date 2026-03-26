import sys
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1]
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

import main


def main_entry() -> int:
    if len(sys.argv) < 2:
        raise SystemExit("usage: python run_top10_notify_once.py <openid>")
    openid = sys.argv[1].strip()
    if not openid:
        raise SystemExit("openid is required")
    main.run_top10_generation_and_notify(openid)
    return 0


if __name__ == "__main__":
    raise SystemExit(main_entry())
