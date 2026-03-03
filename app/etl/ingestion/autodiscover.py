"""Auto-discovery helpers for B3 normalized CSV siblings.

Expose _find_negocios_sibling so callers (scripts/tests) can import it
without manipulating sys.path.
"""
from __future__ import annotations

import re
from pathlib import Path


def _find_negocios_sibling(instruments_path: Path) -> Path | None:
    """Look for the negocios_consolidados sibling in the same folder.

    Strategy:
      1. Extract the date suffix (YYYYMMDD) from the instruments filename.
      2. Look for ``negocios_consolidados_{YYYYMMDD}.normalized.csv`` in the
         same directory.
      3. If not found by exact name, fall back to any
         ``negocios_consolidados_*.normalized.csv`` in the same folder (picks
         the most recent by name, alphabetically last).

    Returns the path if found, else None.
    """
    folder = instruments_path.parent

    # Step 1 – try exact date-matched sibling
    m = re.search(r"(\d{8})", instruments_path.stem)
    if m:
        date_tag = m.group(1)
        candidate = folder / f"negocios_consolidados_{date_tag}.normalized.csv"
        if candidate.exists():
            return candidate

    # Step 2 – fallback: any negocios normalized CSV in the same folder
    siblings = sorted(folder.glob("negocios_consolidados_*.normalized.csv"))
    if siblings:
        return siblings[-1]  # alphabetically last == most recent date

    return None
