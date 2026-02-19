from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import List


@dataclass(frozen=True)
class FundConfig:
    isin: str
    id_instr: str
    ft_symbol: str  # Lo que escribe el usuario: "LU0563745743", "AMEE:GER", "AMEE:GER:EUR", etc.


def load_funds_csv(path: str | Path) -> List[FundConfig]:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"No existe {path}")

    funds: List[FundConfig] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or not {"isin", "idInstr"}.issubset(set(reader.fieldnames)):
            raise ValueError(f"{path} debe tener cabeceras: isin,idInstr,ft_symbol")

        for row in reader:
            isin = (row.get("isin") or "").strip()
            id_instr = (row.get("idInstr") or "").strip()
            # ft_symbol: lo que escribe el usuario (puede estar vacío → se usará el ISIN)
            ft_symbol = (row.get("ft_symbol") or "").strip() or isin

            if not isin or not id_instr:
                continue

            funds.append(FundConfig(isin=isin, id_instr=id_instr, ft_symbol=ft_symbol))

    # Deduplicar por ISIN (última línea gana)
    dedup = {f.isin: f for f in funds}
    return list(dedup.values())
