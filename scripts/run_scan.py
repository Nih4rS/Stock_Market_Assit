#!/usr/bin/env python3
import os
import sys
from pathlib import Path

# Ensure src/ is on sys.path for local runs without installation
repo_root = Path(__file__).resolve().parents[1]
src_path = repo_root / "src"
if src_path.exists():
    sys.path.insert(0, str(src_path))

from smassist.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
