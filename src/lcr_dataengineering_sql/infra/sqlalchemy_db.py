from __future__ import annotations
from typing import Mapping, Any, Iterator
from contextlib import contextmanager
import pandas as pd
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine, Connection
from ..core.ports import Db

def _b(name: str) -> str:
    return f"[{name.replace(']', ']]')}]"

def _fqtn(schema: str, table: str) -> str:
    return f"{_b(schema)}.{_b(table)}"

class SqlAlchemyDb(Db):
    def __init__(self, engine_provider: callable[[], Engine]):
        self._engine_provider = engine_provider

    @property
    def engine(self) -> Engine:
        return self._engine_provider()

    # ---------------- Execuções básicas ----------------

    def execute(self, sql: str, params: Mapping[str, Any] | None = None) -> int:
        with self.engine.begin() as conn:
            res = conn.execute(text(sql), params or {})
            return res.rowcount or 0

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

    # ---------------- Schema / Tabela / View / Procedure ----------------

    def create_schema(self, schema: str, if_not_exists: bool = True) -> None:
        if if_not_exists:
            self.execute(f"IF SCHEMA_ID(N'{schema}') IS NULL EXEC('CREATE SCHEMA {_b(schema)}');")
        else:
            self.execute(f"CREATE SCHEMA {_b(schema)};")

    def truncate_table(self, schema: str, table: str) -> None:
        self.execute(f"TRUNCATE TABLE {_fqtn(schema, table)};")

    def drop_table(self, schema: str, table: str, if_exists: bool = True) -> None:
        if if_exists:
            self.execute(f"IF OBJECT_ID(N'{schema}.{table}', N'U') IS NOT NULL DROP TABLE {_fqtn(schema, table)};")
        else:
            self.execute(f"DROP TABLE {_fqtn(schema, table)};")

    def table_exists(self, schema: str, table: str) -> bool:
        sql = """
        SELECT 1
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
        """
        return len(self.query_all(sql, {"schema": schema, "table": table})) > 0

    def create_view(self, schema: str, view: str, select_sql: str, or_replace: bool = True) -> None:
        if or_replace:
            self.execute(f"IF OBJECT_ID(N'{schema}.{view}', N'V') IS NOT NULL DROP VIEW {_fqtn(schema, view)};")
        self.execute(f"CREATE VIEW {_fqtn(schema, view)} AS {select_sql};")

    def drop_view(self, schema: str, view: str, if_exists: bool = True) -> None:
        if if_exists:
            self.execute(f"IF OBJECT_ID(N'{schema}.{view}', N'V') IS NOT NULL DROP VIEW {_fqtn(schema, view)};")
        else:
            self.execute(f"DROP VIEW {_fqtn(schema, view)};")

    def create_procedure(self, schema: str, proc: str, definition_sql: str, or_alter: bool = True) -> None:
        """
        definition_sql deve conter o corpo da proc (ex.: parâmetros + AS BEGIN ... END).
        Se or_alter=True, cria se não existe, senão ALTER.
        """
        if or_alter:
            sql = f"""
            IF OBJECT_ID(N'{schema}.{proc}', N'P') IS NULL
                EXEC('CREATE PROCEDURE {_fqtn(schema, proc)} AS BEGIN SET NOCOUNT ON; RETURN; END');
            ALTER PROCEDURE {_fqtn(schema, proc)} {definition_sql}
            """
            self.execute(sql)
        else:
            self.execute(f"CREATE PROCEDURE {_fqtn(schema, proc)} {definition_sql}")

    def exec_procedure(self, schema: str, proc: str, params: Mapping[str, Any] | None = None) -> list[dict]:
        """
        Executa proc via EXEC com parâmetros nomeados.
        Ex.: params={'@id': 1, '@nome': 'x'}
        """
        params = params or {}
        named = ", ".join(f"{k} = :{k[1:]}" if k.startswith("@") else f"@{k} = :{k}" for k in params.keys())
        sql = f"EXEC {_fqtn(schema, proc)} {named}" if named else f"EXEC {_fqtn(schema, proc)}"
        return self.query_all(sql, params)

    # ---------------- Criação/ingestão de dados ----------------

    def create_table_from_query(self, create_table_sql: str) -> None:
        """Aceita um CREATE TABLE ... (ou SELECT INTO ...) bruto."""
        self.execute(create_table_sql)

    def create_table_from_df(self, df: pd.DataFrame, schema: str, table: str,
                             pk: list[str] | None = None, if_not_exists: bool = True) -> bool:
        if if_not_exists and self.table_exists(schema, table):
            return False
        # cria tabela vazia com os dtypes inferidos pelo pandas/sqlalchemy
        empty = df.iloc[0:0]
        with self.engine.begin() as conn:
            empty.to_sql(name=table, con=conn, schema=schema, if_exists="fail", index=False)
            if pk:
                cols = ", ".join(_b(c) for c in pk)
                self.execute(f"ALTER TABLE {_fqtn(schema, table)} ADD CONSTRAINT {_b('PK_'+table)} PRIMARY KEY ({cols});")
        return True

    def insert_df(self, df: pd.DataFrame, schema: str, table: str, chunksize: int = 10000) -> int:
        with self.engine.begin() as conn:
            conn = conn.execution_options(fast_executemany=True)
            df.to_sql(name=table, con=conn, schema=schema, if_exists="append", index=False, chunksize=chunksize)
            return len(df)

    # ------------ transação opcional (se quiser compartilhar) ------------
    @contextmanager
    def transaction(self):
        with self.engine.begin() as conn:
            yield _TxDb(conn)

class _TxDb(SqlAlchemyDb):
    def __init__(self, conn: Connection):
        super().__init__(engine_provider=lambda: conn.engine)
        self._conn = conn

    def execute(self, sql: str, params: Mapping[str, Any] | None = None) -> int:
        res = self._conn.execute(text(sql), params or {})
        return res.rowcount or 0

    def query_all(self, sql: str, params: Mapping[str, Any] | None = None) -> list[dict]:
        res = self._conn.execute(text(sql), params or {})
        return res.mappings().all()

    def query_iter(self, sql: str, params: Mapping[str, Any] | None = None):
        res = self._conn.execute(text(sql), params or {})
        for row in res.mappings():
            yield row

    def insert_df(self, df: pd.DataFrame, schema: str, table: str, chunksize: int = 10000) -> int:
        conn = self._conn.execution_options(fast_executemany=True)
        df.to_sql(name=table, con=conn, schema=schema, if_exists="append", index=False, chunksize=chunksize)
        return len(df)
