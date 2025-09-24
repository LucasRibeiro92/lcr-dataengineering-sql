# src/lcr_dataengineering_sql/infra/sqlalchemy_db.py
from __future__ import annotations
from typing import Mapping, Any, Iterator
from contextlib import contextmanager
import pandas as pd
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine, Connection

from ..core.ports import Db

class SqlAlchemyDb(Db):
    def __init__(self, engine_provider: callable[[], Engine]):
        self._engine_provider = engine_provider

    @property
    def engine(self) -> Engine:
        return self._engine_provider()

    def execute(self, sql: str, params: Mapping[str, Any] | None = None) -> int:
        with self.engine.begin() as conn:
            res = conn.execute(text(sql), params or {})
            return res.rowcount if res.rowcount is not None else 0

    def query_all(self, sql: str, params: Mapping[str, Any] | None = None) -> list[dict]:
        with self.engine.connect() as conn:
            res = conn.execute(text(sql), params or {})
            return res.mappings().all()

    def query_iter(self, sql: str, params: Mapping[str, Any] | None = None) -> Iterator[dict]:
        conn = self.engine.connect()
        try:
            res = conn.execute(text(sql), params or {})
            for row in res.mappings():
                yield row
        finally:
            conn.close()

    def bulk_insert_df(self, df: pd.DataFrame, schema: str, table: str, chunksize: int = 10000) -> int:
        total = 0
        with self.engine.begin() as conn:
            conn = conn.execution_options(fast_executemany=True)
            df.to_sql(
                name=table,
                con=conn,
                schema=schema,
                if_exists="append",
                index=False,
                chunksize=chunksize,
            )
            total += len(df)
        return total

    @contextmanager
    def transaction(self):
        with self.engine.begin() as conn:
            # expõe métodos no contexto, mas reutilizando a mesma conexão/tx
            yield _TxDb(conn)

class _TxDb(SqlAlchemyDb):
    """Implementação que reusa uma Connection já aberta em transação."""
    def __init__(self, conn: Connection):
        # engine_provider “fake” só para cumprir assinatura
        super().__init__(engine_provider=lambda: conn.engine)
        self._conn = conn

    def execute(self, sql: str, params: Mapping[str, Any] | None = None) -> int:
        res = self._conn.execute(text(sql), params or {})
        return res.rowcount if res.rowcount is not None else 0

    def query_all(self, sql: str, params: Mapping[str, Any] | None = None) -> list[dict]:
        res = self._conn.execute(text(sql), params or {})
        return res.mappings().all()

    def query_iter(self, sql: str, params: Mapping[str, Any] | None = None):
        res = self._conn.execute(text(sql), params or {})
        for row in res.mappings():
            yield row

    def bulk_insert_df(self, df: pd.DataFrame, schema: str, table: str, chunksize: int = 10000) -> int:
        conn = self._conn.execution_options(fast_executemany=True)
        df.to_sql(
            name=table,
            con=conn,
            schema=schema,
            if_exists="append",
            index=False,
            chunksize=chunksize,
        )
        return len(df)
