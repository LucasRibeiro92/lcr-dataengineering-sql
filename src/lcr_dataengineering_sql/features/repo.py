# src/lcr_dataengineering_sql/features/repo.py
from __future__ import annotations
from typing import Iterable
import pandas as pd
from ..core.ports import Db

def _bracket_ident(name: str) -> str:
    """
    Escapa identificadores para SQL Server: coloca [ ] e duplica ']' internas.
    Ex.: schema -> [schema],  weird]name -> [weird]]name]
    """
    return f"[{name.replace(']', ']]')}]"

def _fqtn(schema: str, table: str) -> str:
    """Fully Qualified Table Name -> [schema].[table]."""
    return f"{_bracket_ident(schema)}.{_bracket_ident(table)}"

class Repo:
    """
    Repositório genérico. Você pode:
      - passar schema/table como defaults no __init__, e/ou
      - passar schema/table por método (sobrepõe os defaults).
    """
    def __init__(self, db: Db, default_schema: str | None = None, default_table: str | None = None):
        self.db = db
        self.default_schema = default_schema
        self.default_table = default_table

    # ------- helpers -------
    def _resolve(self, schema: str | None, table: str | None) -> tuple[str, str]:
        s = schema or self.default_schema
        t = table or self.default_table
        if not s or not t:
            raise ValueError("schema e table devem ser informados (no __init__ ou no método).")
        return s, t

    # ------- operações de leitura/escrita genéricas -------

    def listar_top(
        self,
        n: int = 10,
        columns: Iterable[str] | str = "*",
        schema: str | None = None,
        table: str | None = None,
        where: str | None = None,
        order_by: str | None = None,
    ) -> list[dict]:
        """
        SELECT TOP (n) ... FROM [schema].[table]
        columns: sequência de nomes OU "*" (default).
        where: string opcional (sem 'WHERE').
        order_by: string opcional (sem 'ORDER BY').
        """
        s, t = self._resolve(schema, table)
        cols = (
            "*"
            if columns == "*" or columns is None
            else ", ".join(_bracket_ident(c) for c in columns)
        )
        sql = [f"SELECT TOP ({int(n)}) {cols} FROM {_fqtn(s, t)}"]
        if where:
            sql.append(f"WHERE {where}")
        if order_by:
            sql.append(f"ORDER BY {order_by}")
        return self.db.query_all(" ".join(sql))

    def count(self, schema: str | None = None, table: str | None = None) -> int:
        s, t = self._resolve(schema, table)
        row = self.db.query_all(f"SELECT COUNT(*) AS cnt FROM {_fqtn(s, t)}")[0]
        return int(row["cnt"])

    def inserir_lote(
        self,
        df: pd.DataFrame,
        schema: str | None = None,
        table: str | None = None,
        chunksize: int = 10000,
    ) -> int:
        s, t = self._resolve(schema, table)
        return self.db.bulk_insert_df(df, schema=s, table=t, chunksize=chunksize)

    def truncate(self, schema: str | None = None, table: str | None = None) -> None:
        s, t = self._resolve(schema, table)
        self.db.execute(f"TRUNCATE TABLE {_fqtn(s, t)}")

    def drop(self, schema: str | None = None, table: str | None = None, if_exists: bool = True) -> None:
        s, t = self._resolve(schema, table)
        if if_exists:
            # SQL Server não tem "DROP TABLE IF EXISTS" em versões muito antigas; aqui vai padrão moderno:
            self.db.execute(f"IF OBJECT_ID(N'{s}.{t}', N'U') IS NOT NULL DROP TABLE {_fqtn(s, t)};")
        else:
            self.db.execute(f"DROP TABLE {_fqtn(s, t)}")
