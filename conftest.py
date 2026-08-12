"""Root pytest conftest — puts the repo root on sys.path.

Without this file, `from contracts.play_event_v1 import ...` and
`from src.create_topics import ...` fail when pytest collects anything under
`tests/`, because the repo root is not otherwise on the import path.

We insert the directory explicitly rather than leaning on pytest's implicit
rootdir insertion: that behaviour depends on rootdir detection and on the
absence of `__init__.py` files, both of which can change underneath us. An
explicit insert is the same one line and cannot silently stop working.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
