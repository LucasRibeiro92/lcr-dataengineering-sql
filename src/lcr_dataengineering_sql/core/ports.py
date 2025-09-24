# src/lcr_dataengineering_sql/core/ports.py
from __future__ import annotations
from typing import Protocol, Mapping, Any, Iterable, Iterator
import pandas as pd
from contextlib import AbstractContextManager

class Db(Protocol):
    def execute(self, sql: str, params: Mapping[str, Any] | None = None) -> int:
        """DDL/DML. Retorna rowcount."""
        ...

    def query_all(self, sql: str, params: Mapping[str, Any] | None = None) -> list[dict]:
        """SELECT → lista de dicts."""
        ...

    def query_iter(self, sql: str, params: Mapping[str, Any] | None = None) -> Iterator[dict]:
        """SELECT streaming (iterável de dicts)."""
        ...

    def bulk_insert_df(self, df: pd.DataFrame, schema: str, table: str, chunksize: int = 10000) -> int:
        """Insert em lote via pandas, retorna total inserido."""
        ...

    def transaction(self) -> AbstractContextManager[Db]:
        """Contexto transacional: with db.transaction(): db.execute(...)."""
        ...
