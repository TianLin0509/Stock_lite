from __future__ import annotations

from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / "deploy_bundle.zip"

EXCLUDE_DIRS = {
    ".git",
    "__pycache__",
    "logs",
    "storage",
    "tests",
    "top10",
    "ui",
    "pages",
    "user_data",
    ".pytest_cache",
}

EXCLUDE_SUFFIXES = {".pyc", ".pyo", ".log"}


def should_include(path: Path) -> bool:
    rel = path.relative_to(ROOT)
    for part in rel.parts:
        if part in EXCLUDE_DIRS:
            return False
    if path.is_file() and path.suffix.lower() in EXCLUDE_SUFFIXES:
        return False
    if path.name == "deploy_bundle.zip":
        return False
    return True


def main() -> None:
    if OUTPUT.exists():
        OUTPUT.unlink()

    with ZipFile(OUTPUT, "w", ZIP_DEFLATED) as zf:
        for path in ROOT.rglob("*"):
            if not should_include(path):
                continue
            if path.is_dir():
                continue
            zf.write(path, path.relative_to(ROOT))

    print(OUTPUT)


if __name__ == "__main__":
    main()
