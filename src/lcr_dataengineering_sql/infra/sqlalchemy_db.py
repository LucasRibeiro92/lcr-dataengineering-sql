from __future__ import annotations
from typing import Mapping, Any, Iterator
from contextlib import contextmanager
import pandas as pd
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine, Connection
from ..core.ports import Db

def _dialect_name(engine: Engine) -> str:
    return engine.dialect.name  # "mssql" | "postgresql" | "mysql" | ...

def _quote(engine: Engine, ident: str) -> str:
    # usa o preparer do dialect para quotar corretamente cada banco
    prep = engine.dialect.identifier_preparer
    return prep.quote(ident)

def _fqtn(engine: Engine, schema: str, table: str) -> str:
    if schema:
        return f"{_quote(engine, schema)}.{_quote(engine, table)}"
    return _quote(engine, table)

class SqlAlchemyDb(Db):
    def __init__(self, engine_provider: callable[[], Engine]):
        self._engine_provider = engine_provider

    @property
    def engine(self) -> Engine:
        return self._engine_provider()

    def ping(self) -> bool:
        """
        Faz um round-trip simples no banco.
        Usa SELECT 1 (serve para mssql, postgres e mysql).
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    # -------- básicos --------
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

    # -------- schema / tabela / view / proc --------
    def create_schema(self, schema: str, if_not_exists: bool = True) -> None:
        d = _dialect_name(self.engine)
        if d == "mssql":
            if if_not_exists:
                self.execute(f"IF SCHEMA_ID(N'{schema}') IS NULL EXEC('CREATE SCHEMA {_quote(self.engine, schema)}');")
            else:
                self.execute(f"CREATE SCHEMA {_quote(self.engine, schema)};")
        elif d == "postgresql":
            self.execute(f"CREATE SCHEMA {'IF NOT EXISTS ' if if_not_exists else ''}{_quote(self.engine, schema)};")
        elif d == "mysql":
            # em MySQL, schema = database
            self.execute(f"CREATE DATABASE {'IF NOT EXISTS ' if if_not_exists else ''}{_quote(self.engine, schema)};")
        else:
            raise NotImplementedError(f"create_schema não implementado para {d}")

    def truncate_table(self, schema: str, table: str) -> None:
        d = _dialect_name(self.engine)
        fq = _fqtn(self.engine, schema, table)
        if d in ("mssql", "postgresql", "mysql"):
            self.execute(f"TRUNCATE TABLE {fq};")
        else:
            raise NotImplementedError(f"truncate_table não implementado para {d}")

    def drop_table(self, schema: str, table: str, if_exists: bool = True) -> None:
        d = _dialect_name(self.engine)
        fq = _fqtn(self.engine, schema, table)
        if d == "mssql":
            if if_exists:
                self.execute(f"IF OBJECT_ID(N'{schema}.{table}', N'U') IS NOT NULL DROP TABLE {fq};")
            else:
                self.execute(f"DROP TABLE {fq};")
        elif d == "postgresql":
            self.execute(f"DROP TABLE {'IF EXISTS ' if if_exists else ''}{fq} CASCADE;")
        elif d == "mysql":
            self.execute(f"DROP TABLE {'IF EXISTS ' if if_exists else ''}{fq};")
        else:
            raise NotImplementedError(f"drop_table não implementado para {d}")

    def table_exists(self, schema: str, table: str) -> bool:
        insp = inspect(self.engine)
        return insp.has_table(table, schema=schema)

    def create_view(self, schema: str, view: str, select_sql: str, or_replace: bool = True) -> None:
        d = _dialect_name(self.engine)
        fq = _fqtn(self.engine, schema, view)
        if d == "mssql":
            if or_replace:
                self.execute(f"IF OBJECT_ID(N'{schema}.{view}', N'V') IS NOT NULL DROP VIEW {fq};")
            self.execute(f"CREATE VIEW {fq} AS {select_sql};")
        elif d in ("postgresql", "mysql"):
            self.execute(f"{'CREATE OR REPLACE' if or_replace else 'CREATE'} VIEW {fq} AS {select_sql};")
        else:
            raise NotImplementedError(f"create_view não implementado para {d}")

    def drop_view(self, schema: str, view: str, if_exists: bool = True) -> None:
        d = _dialect_name(self.engine)
        fq = _fqtn(self.engine, schema, view)
        if d == "mssql":
            if if_exists:
                self.execute(f"IF OBJECT_ID(N'{schema}.{view}', N'V') IS NOT NULL DROP VIEW {fq};")
            else:
                self.execute(f"DROP VIEW {fq};")
        elif d == "postgresql":
            self.execute(f"DROP VIEW {'IF EXISTS ' if if_exists else ''}{fq} CASCADE;")
        elif d == "mysql":
            self.execute(f"DROP VIEW {'IF EXISTS ' if if_exists else ''}{fq};")
        else:
            raise NotImplementedError(f"drop_view não implementado para {d}")

    def create_procedure(self, schema: str, proc: str, definition_sql: str, or_alter: bool = True) -> None:
        d = _dialect_name(self.engine)
        fq = _fqtn(self.engine, schema, proc)
        if d == "mssql":
            if or_alter:
                self.execute(f"""
                IF OBJECT_ID(N'{schema}.{proc}', N'P') IS NULL
                    EXEC('CREATE PROCEDURE {fq} AS BEGIN SET NOCOUNT ON; RETURN; END');
                ALTER PROCEDURE {fq} {definition_sql}
                """)
            else:
                self.execute(f"CREATE PROCEDURE {fq} {definition_sql}")
        elif d == "postgresql":
            # PG tem FUNCTION e PROCEDURE; PROCEDURE (CALL) existe >= v11.
            # Aqui assumimos PROCEDURE + CALL.
            if or_alter:
                # não há "CREATE OR REPLACE PROCEDURE"; faremos drop + create
                self.execute(f"DROP PROCEDURE IF EXISTS {fq} CASCADE;")
            self.execute(f"CREATE PROCEDURE {fq} {definition_sql};")
        elif d == "mysql":
            if or_alter:
                self.execute(f"DROP PROCEDURE IF EXISTS {fq};")
            self.execute(f"CREATE PROCEDURE {fq} {definition_sql};")
        else:
            raise NotImplementedError(f"create_procedure não implementado para {d}")

    def exec_procedure(self, schema: str, proc: str, params: Mapping[str, Any] | None = None) -> list[dict]:
        d = _dialect_name(self.engine)
        params = params or {}
        # monta lista de nomeados "nome=:nome"
        named = ", ".join(f"{k}=:${k}" if d=="postgresql" else f"{k}=:{k.lstrip('@')}" for k in params.keys())

        if d == "mssql":
            # EXEC [schema].[proc] @p=:p
            sql = f"EXEC {_fqtn(self.engine, schema, proc)} {named}" if named else f"EXEC {_fqtn(self.engine, schema, proc)}"
            # tira '@' do dict
            clean = {k.lstrip('@'): v for k, v in params.items()}
            return self.query_all(sql, clean)
        elif d == "postgresql":
            # CALL schema.proc(:p1,:p2) — em PG os params são posicionais ou nomeados com =>?
            # Usaremos nomeados: CALL sch.proc(p=>:p, x=>:x)
            named_call = ", ".join(f"{k}=>:{k}" for k in params.keys())
            sql = f"CALL {_fqtn(self.engine, schema, proc)}({named_call});" if params else f"CALL {_fqtn(self.engine, schema, proc)}();"
            return self.query_all(sql, params)
        elif d == "mysql":
            # CALL schema.proc(:p1,:p2) – nomeados não são padrão; convertemos para posicionais
            if params:
                placeholders = ", ".join([f":p{i}" for i,_ in enumerate(params, start=1)])
                ordered = {f"p{i}": v for i, v in enumerate(params.values(), start=1)}
                sql = f"CALL {_fqtn(self.engine, schema, proc)}({placeholders});"
                return self.query_all(sql, ordered)
            else:
                sql = f"CALL {_fqtn(self.engine, schema, proc)}();"
                return self.query_all(sql)
        else:
            raise NotImplementedError(f"exec_procedure não implementado para {d}")

    # -------- criar/ingestar dados --------
    def create_table_from_query(self, create_table_sql: str) -> None:
        self.execute(create_table_sql)

    def create_table_from_df(self, df: pd.DataFrame, schema: str, table: str,
                             pk: list[str] | None = None, if_not_exists: bool = True) -> bool:
        if if_not_exists and self.table_exists(schema, table):
            return False
        empty = df.iloc[0:0]
        with self.engine.begin() as conn:
            empty.to_sql(name=table, con=conn, schema=schema, if_exists="fail", index=False)
            if pk:
                fq = _fqtn(self.engine, schema, table)
                cols = ", ".join(_quote(self.engine, c) for c in pk)
                d = _dialect_name(self.engine)
                if d in ("mssql", "postgresql", "mysql"):
                    conn.execute(text(f"ALTER TABLE {fq} ADD CONSTRAINT {_quote(self.engine, 'PK_'+table)} PRIMARY KEY ({cols});"))
                else:
                    raise NotImplementedError(f"PRIMARY KEY não implementado para {d}")
        return True

    def insert_df(self, df: pd.DataFrame, schema: str, table: str, chunksize: int = 10000) -> int:
        with self.engine.begin() as conn:
            conn = conn.execution_options(fast_executemany=True)
            df.to_sql(name=table, con=conn, schema=schema, if_exists="append", index=False, chunksize=chunksize)
            return len(df)

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

    def query_all(self, sql: str, params: Mapping[str, Any] | None = None):
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
