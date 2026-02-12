from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass(frozen=True)
class FundConfig:
    isin: str
    id_instr: str
    ft_symbol: str  # e.g. "LU0563745743:EUR"


def load_funds_csv(path: str | Path) -> List[FundConfig]:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"No existe {path}")

    funds: List[FundConfig] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        required = {"isin", "idInstr"}
        if reader.fieldnames is None or not required.issubset(set(reader.fieldnames)):
            raise ValueError(f"{path} debe tener cabeceras exactas: isin,idInstr,ft_symbol (ft_symbol opcional)")

        for row in reader:
            isin = (row.get("isin") or "").strip()
            id_instr = (row.get("idInstr") or "").strip()
            ft_symbol = (row.get("ft_symbol") or "").strip()

            if not isin or not id_instr:
                continue

            if not ft_symbol:
                ft_symbol = f"{isin}:EUR"

            funds.append(FundConfig(isin=isin, id_instr=id_instr, ft_symbol=ft_symbol))

    # Evitar duplicados por ISIN (última línea gana)
    dedup = {}
    for fcfg in funds:
        dedup[fcfg.isin] = fcfg
    return list(dedup.values())
