from __future__ import annotations
from typing import Dict, Optional, Iterable
from sqlalchemy import text, inspect
from sqlalchemy.types import Integer, Float, Boolean, DateTime, String
from sqlalchemy import text
from .engine import get_engine
import pandas as pd

def ping() -> dict:
    eng = get_engine()
    with eng.connect() as conn:
        row = conn.execute(text("SELECT @@SERVERNAME AS server_name, DB_NAME() AS db, SUSER_SNAME() AS login")).one()
        return dict(row._mapping)

def exec_sql(sql: str, params: dict | None = None) -> int:
    """
    DDL/DML (CREATE/INSERT/UPDATE/DELETE). Retorna rowcount.
    """
    eng = get_engine()
    with eng.begin() as conn:             # abre transação e comita no final
        res = conn.execute(text(sql), params or {})
        return res.rowcount

def query_all(sql: str, params: dict | None = None):
    """
    SELECT que já faz o fetch antes de fechar a conexão. Retorna lista de dict-like.
    """
    eng = get_engine()
    with eng.connect() as conn:
        res = conn.execute(text(sql), params or {})
        return res.mappings().all()       # <- consome antes de fechar

def query_one(sql: str, params: dict | None = None):
    eng = get_engine()
    with eng.connect() as conn:
        res = conn.execute(text(sql), params or {})
        return res.mappings().one()

def table_exists(schema: str, table: str) -> bool:
    eng = get_engine()
    insp = inspect(eng)
    return insp.has_table(table_name=table, schema=schema)

def infer_sqlalchemy_dtypes(df, max_varchar: int = 4000) -> Dict[str, String | Integer | Float | Boolean | DateTime]:
    """
    Mapeia dtypes do pandas -> SQLAlchemy para SQL Server.
    Strings ganham tamanho baseado no comprimento máximo observado (até max_varchar).
    Se exceder muito, usa NVARCHAR(MAX).
    """
    dtypes: Dict[str, object] = {}
    for col in df.columns:
        s = df[col]
        if pd.api.types.is_integer_dtype(s):
            dtypes[col] = Integer()
        elif pd.api.types.is_float_dtype(s):
            dtypes[col] = Float()
        elif pd.api.types.is_bool_dtype(s):
            dtypes[col] = Boolean()
        elif pd.api.types.is_datetime64_any_dtype(s):
            dtypes[col] = DateTime()
        else:
            # trata como texto
            # estima tamanho a partir do lote (não varre arquivo inteiro aqui)
            try:
                maxlen = int(s.astype(str).map(lambda x: 0 if x is None else len(x)).max())
            except Exception:
                maxlen = 255
            if maxlen == 0:
                maxlen = 1
            if maxlen > max_varchar:
                # NVARCHAR(MAX)
                dtypes[col] = String().with_variant(String(length=None), "mssql")
            else:
                dtypes[col] = String(length=max(1, min(max_varchar, maxlen)))
    return dtypes

def create_table_if_not_exists_from_df(df, schema: str, table: str, pk: Optional[Iterable[str]] = None, sample_rows: int = 5000):
    """
    Cria a tabela com base no DF (amostra) se ela não existir.
    Para PK, passe uma lista de colunas (opcional).
    """

    eng = get_engine()

    if table_exists(schema, table):
        return False  # já existe

    # usa uma amostra para inferir tamanhos de string
    if len(df) > sample_rows:
        df_sample = df.head(sample_rows).copy()
    else:
        df_sample = df

    dtypes = infer_sqlalchemy_dtypes(df_sample)

    # deixa o pandas criar a tabela com os dtypes que definimos
    # criamos tabela vazia usando 0 linhas
    empty = df.iloc[0:0]
    with eng.begin() as conn:
        empty.to_sql(
            name=table,
            con=conn,
            schema=schema,
            if_exists="fail",
            index=False,
            dtype=dtypes,
        )
        # define PK se solicitado
        if pk:
            # monta um ALTER TABLE ADD CONSTRAINT PK
            pk_cols = ", ".join(f"[{c}]" for c in pk)
            sql = f"ALTER TABLE [{schema}].[{table}] ADD CONSTRAINT [PK_{table}] PRIMARY KEY ({pk_cols})"
            conn.execute(text(sql))
    return True

def bulk_insert_df(df, schema: str, table: str, chunksize: int = 10000):
    """
    Insert em lote estável para SQL Server (pyodbc) usando fast_executemany.
    Não usar method="multi" (causa 07002 em alguns cenários).
    """
    eng = get_engine()
    with eng.begin() as conn:
        # ativa o modo rápido do pyodbc
        conn = conn.execution_options(fast_executemany=True)
        df.to_sql(
            name=table,
            con=conn,
            schema=schema,
            if_exists="append",
            index=False,
            # method=None -> executemany padrão do SQLAlchemy (compatível com fast_executemany)
            chunksize=chunksize,
        )

