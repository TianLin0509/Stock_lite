from __future__ import annotations

import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
ROOT_STR = str(ROOT_DIR)
if ROOT_STR not in sys.path:
    sys.path.insert(0, ROOT_STR)
