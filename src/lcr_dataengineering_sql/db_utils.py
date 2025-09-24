from __future__ import annotations
import re
from typing import Dict, Optional, Iterable
from sqlalchemy import text, inspect
from sqlalchemy.types import Integer, Float, Boolean, DateTime, String
from sqlalchemy import text
from .engine import default_engine_provider
import pandas as pd

def ping() -> dict:
    eng = default_engine_provider()
    with eng.connect() as conn:
        row = conn.execute(text("SELECT @@SERVERNAME AS server_name, DB_NAME() AS db, SUSER_SNAME() AS login")).one()
        return dict(row._mapping)

'''
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

def _sanitize_identifier(name: str) -> str:
    """
    Mantém apenas A-Z, 0-9 e _, trocando os demais por _.
    Se começar com dígito, prefixa 'C_'.
    Remove underscores extras nas pontas.
    """
    n = re.sub(r'[^A-Z0-9_]', '_', name.upper())
    n = n.strip('_')
    if not n:
        n = 'C'
    if n[0].isdigit():
        n = 'C_' + n
    return n

def _build_column_mapping(cols: Iterable[str], prefix: str) -> Dict[str, str]:
    """
    Gera mapeamento original -> PREFIXO+UPPER+saneado, evitando colisões.
    """
    seen = set()
    mapping: Dict[str, str] = {}
    for c in cols:
        base = _sanitize_identifier(f"{prefix}{str(c)}")
        cand = base
        k = 1
        while cand in seen:
            k += 1
            cand = f"{base}_{k}"
        seen.add(cand)
        mapping[c] = cand
    return mapping

def create_table_if_not_exists_from_df(
    df,
    schema: str,
    table: str,
    column_prefix: str,
    pk: Optional[Iterable[str]] = None,
    sample_rows: int = 5000
) -> bool:
    """
    Cria a tabela com base no DF (amostra) se ela não existir.
    - Colunas sobem para MAIÚSCULO
    - Recebem prefixo `column_prefix`
    - Nomes são saneados para identificadores válidos no SQL Server

    Retorna True se criou, False se já existia.
    """
    eng = get_engine()

    if table_exists(schema, table):
        return False  # já existe

    # amostra para inferência de tipos
    df_sample = df.head(sample_rows).copy() if len(df) > sample_rows else df.copy()

    # mapeia nomes de colunas -> PREFIXO + UPPER + saneado
    col_map = _build_column_mapping(df_sample.columns, column_prefix)

    # inferência dos tipos com base na amostra original
    dtypes = infer_sqlalchemy_dtypes(df_sample)
    # reindexa o dict de dtypes para os nomes novos
    dtypes_renamed = {col_map[k]: v for k, v in dtypes.items()}

    # cria DataFrame vazio já com colunas renomeadas
    empty = df.iloc[0:0].rename(columns=col_map)

    with eng.begin() as conn:
        # cria a tabela
        empty.to_sql(
            name=table,
            con=conn,
            schema=schema,
            if_exists="fail",
            index=False,
            dtype=dtypes_renamed,
        )

        # PK (se houver) – precisa apontar para os nomes novos
        if pk:
            pk_mapped = [_sanitize_identifier(f"{column_prefix}{p}") for p in pk]
            pk_cols = ", ".join(f"[{c}]" for c in pk_mapped)
            sql = f"ALTER TABLE [{schema}].[{table}] ADD CONSTRAINT [PK_{_sanitize_identifier(table)}] PRIMARY KEY ({pk_cols})"
            conn.execute(text(sql))

    return True

# --- ajudinha para usar no insert depois ---

def rename_df_for_db(df, column_prefix: str):
    """
    Renomeia o DF para os mesmos nomes que a função de criação usou.
    Use isto ANTES de chamar bulk_insert_df para bater com os nomes no banco.
    """
    mapping = _build_column_mapping(df.columns, column_prefix)
    return df.rename(columns=mapping)

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

'''