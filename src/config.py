from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import List


@dataclass(frozen=True)
class FundConfig:
    isin: str
    ft_url: str          # Vacío → salta FT
    fundsquare_url: str  # Vacío → salta Fundsquare
    investing_url: str   # Vacío → salta Investing


def load_funds_csv(path: str | Path) -> List[FundConfig]:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"No existe {path}")

    funds: List[FundConfig] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or "isin" not in reader.fieldnames:
            raise ValueError(f"{path} debe tener al menos la cabecera 'isin'")

        for row in reader:
            isin = (row.get("isin") or "").strip()
            if not isin:
                continue
            funds.append(FundConfig(
                isin=isin,
                ft_url=(row.get("ft_url") or "").strip(),
                fundsquare_url=(row.get("fundsquare_url") or "").strip(),
                investing_url=(row.get("investing_url") or "").strip(),
            ))

    # Deduplicar por ISIN (última línea gana)
    return list({f.isin: f for f in funds}.values())
