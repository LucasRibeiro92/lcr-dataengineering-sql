# src/lcr_dataengineering_sql/utils/naming.py
from __future__ import annotations
import re
import pandas as pd
from typing import Dict, Iterable

def _sanitize_identifier(name: str) -> str:
    """
    Mantém apenas A-Z, 0-9 e _, converte para MAIÚSCULO.
    Substitui demais caracteres por '_'. Evita começar com dígito.
    """
    n = re.sub(r'[^A-Z0-9_]', '_', str(name).upper())
    n = n.strip('_') or 'C'
    if n[0].isdigit():
        n = f"C_{n}"
    return n

def build_column_mapping(cols: Iterable[str], prefix: str) -> Dict[str, str]:
    """
    original -> PREFIXO + MAIÚSCULO (saneado). Dedup se colidir.
    """
    seen = set()
    mapping: Dict[str, str] = {}
    for c in cols:
        base = _sanitize_identifier(f"{prefix}{c}")
        cand = base
        i = 1
        while cand in seen:
            i += 1
            cand = f"{base}_{i}"
        seen.add(cand)
        mapping[str(c)] = cand
    return mapping

def rename_df_columns(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """
    Retorna uma cópia do DF com os nomes das colunas renomeados segundo o mapping.
    """
    mapping = build_column_mapping(df.columns, prefix)
    return df.rename(columns=mapping)
