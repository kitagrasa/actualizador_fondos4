from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
KEEPALIVE_FILE = ROOT / ".github" / "keepalive" / "last_keepalive_utc.txt"


def main() -> int:
    KEEPALIVE_FILE.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    new_text = f"{now}\n"
    old_text = KEEPALIVE_FILE.read_text(encoding="utf-8") if KEEPALIVE_FILE.exists() else None
    if old_text == new_text:
        return 0
    KEEPALIVE_FILE.write_text(new_text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
