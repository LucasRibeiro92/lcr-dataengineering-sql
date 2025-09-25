from __future__ import annotations
from typing import Iterable, Mapping, Any
import pandas as pd
from ..core.ports import Db
from ..utils.naming import rename_df_columns

def _b(name: str) -> str:
    return f"[{name.replace(']', ']]')}]"

def _fqtn(schema: str, table: str) -> str:
    return f"{_b(schema)}.{_b(table)}"

class Repo:
    """Fachada genérica, recebe Db e opera em qualquer schema/tabela/view/procedure."""
    def __init__(self, db: Db):
        self.db = db

    # ------------------- Schema / Tabelas -------------------

    def ensure_schema(self, schema: str) -> None:
        self.db.create_schema(schema, if_not_exists=True)

    def create_table_raw(self, create_table_sql: str) -> None:
        self.db.create_table_from_query(create_table_sql)

    def create_table_from_csv(self, csv_path: str, schema: str, table: str,
                              pk: list[str] | None = None, sep=",", encoding="utf-8",
                              decimal=".", parse_dates: list[str] | None = None,
                              sample_rows: int = 100000) -> bool:
        # lê sample primeiro para criar com tipos razoáveis
        df0 = pd.read_csv(csv_path, sep=sep, encoding=encoding, decimal=decimal,
                          parse_dates=parse_dates, nrows=sample_rows, low_memory=False)
        created = self.db.create_table_from_df(df0, schema=schema, table=table, pk=pk, if_not_exists=True)
        # insere sample
        if len(df0):
            self.db.insert_df(df0, schema=schema, table=table)
        # stream restante
        it = pd.read_csv(csv_path, sep=sep, encoding=encoding, decimal=decimal,
                         parse_dates=parse_dates, chunksize=100000, skiprows=range(1, len(df0)+1),
                         low_memory=False)
        for chunk in it:
            if len(chunk):
                self.db.insert_df(chunk, schema=schema, table=table)
        return created

    def create_table_from_parquet(self, parquet_path: str, schema: str, table: str,
                                  pk: list[str] | None = None) -> bool:
        df = pd.read_parquet(parquet_path)  # requer pyarrow
        return self.db.create_table_from_df(df, schema=schema, table=table, pk=pk, if_not_exists=True)

    def insert_csv(self, csv_path: str, schema: str, table: str,
                   sep=",", encoding="utf-8", decimal=".", parse_dates: list[str] | None = None,
                   chunksize: int = 100000) -> int:
        total = 0
        it = pd.read_csv(csv_path, sep=sep, encoding=encoding, decimal=decimal,
                         parse_dates=parse_dates, chunksize=chunksize, low_memory=False)
        for chunk in it:
            if len(chunk):
                total += self.db.insert_df(chunk, schema=schema, table=table, chunksize=chunksize)
        return total

    def insert_parquet(self, parquet_path: str, schema: str, table: str, chunksize: int = 100000) -> int:
        df = pd.read_parquet(parquet_path)
        return self.db.insert_df(df, schema=schema, table=table, chunksize=chunksize)

    def truncate_table(self, schema: str, table: str) -> None:
        self.db.truncate_table(schema, table)

    def drop_table(self, schema: str, table: str, if_exists: bool = True) -> None:
        self.db.drop_table(schema, table, if_exists=if_exists)

    def delete_where(self, schema: str, table: str, where: str, params: Mapping[str, Any] | None = None) -> int:
        sql = f"DELETE FROM {_fqtn(schema, table)} WHERE {where}"
        return self.db.execute(sql, params or {})

    # ------------------- Selects -------------------

    def select_raw(self, sql: str, params: Mapping[str, Any] | None = None) -> list[dict]:
        return self.db.query_all(sql, params)

    def select_top(self, schema: str, table: str, n: int = 10,
                   columns: Iterable[str] | str = "*",
                   where: str | None = None, order_by: str | None = None) -> list[dict]:
        cols = "*" if columns == "*" else ", ".join(_b(c) for c in columns)
        sql = [f"SELECT TOP ({int(n)}) {cols} FROM {_fqtn(schema, table)}"]
        if where:
            sql.append(f"WHERE {where}")
        if order_by:
            sql.append(f"ORDER BY {order_by}")
        return self.db.query_all(" ".join(sql))

    def count(self, schema: str, table: str) -> int:
        row = self.db.query_all(f"SELECT COUNT(*) AS cnt FROM {_fqtn(schema, table)}")[0]
        return int(row["cnt"])

    # ------------------- Views -------------------

    def create_view(self, schema: str, view: str, select_sql: str, or_replace: bool = True) -> None:
        self.db.create_view(schema, view, select_sql, or_replace=or_replace)

    def drop_view(self, schema: str, view: str, if_exists: bool = True) -> None:
        self.db.drop_view(schema, view, if_exists=if_exists)

    def select_view(self, schema: str, view: str, n: int | None = None) -> list[dict]:
        base = f"SELECT * FROM {_fqtn(schema, view)}"
        sql = f"SELECT TOP ({n}) * FROM {_fqtn(schema, view)}" if n else base
        return self.db.query_all(sql)

    # ------------------- Procedures -------------------

    def create_procedure(self, schema: str, proc: str, definition_sql: str, or_alter: bool = True) -> None:
        self.db.create_procedure(schema, proc, definition_sql, or_alter=or_alter)

    def exec_procedure(self, schema: str, proc: str, params: Mapping[str, Any] | None = None) -> list[dict]:
        return self.db.exec_procedure(schema, proc, params or {})

    def create_table_from_csv_with_prefix(
            self,
            csv_path: str,
            schema: str,
            table: str,
            column_prefix: str,
            pk: list[str] | None = None,
            sep=",",
            encoding="utf-8",
            decimal=".",
            parse_dates: list[str] | None = None,
            sample_rows: int = 100_000,
            chunksize_insert: int = 100_000,
    ) -> bool:
        """
        Cria a tabela a partir de um CSV, renomeando colunas (PREFIXO + MAIÚSCULO + saneado),
        e já insere todos os dados.
        """
        # 1) lê um sample para inferir tipos e criar a tabela
        df0 = pd.read_csv(
            csv_path,
            sep=sep,
            encoding=encoding,
            decimal=decimal,
            parse_dates=parse_dates,
            nrows=sample_rows,
            low_memory=False,
        )
        df0_db = rename_df_columns(df0, prefix=column_prefix)

        created = self.db.create_table_from_df(df0_db, schema=schema, table=table, pk=pk, if_not_exists=True)

        return created

    def insert_csv_with_prefix(
            self,
            csv_path: str,
            schema: str,
            table: str,
            column_prefix: str,
            sep=",",
            encoding="utf-8",
            decimal=".",
            parse_dates: list[str] | None = None,
            chunksize: int = 100_000,
    ) -> int:
        """
        Apenas insere (não cria). Renomeia colunas do CSV com o mesmo algoritmo do create_table_from_csv_with_prefix.
        """
        total = 0
        it = pd.read_csv(
            csv_path,
            sep=sep,
            encoding=encoding,
            decimal=decimal,
            parse_dates=parse_dates,
            chunksize=chunksize,
            low_memory=False,
        )
        for chunk in it:
            if not len(chunk):
                continue
            chunk_db = rename_df_columns(chunk, prefix=column_prefix)
            total += self.db.insert_df(chunk_db, schema=schema, table=table, chunksize=chunksize)
        return total
