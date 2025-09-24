# scripts/csv_to_table_static.py

import os
import pandas as pd
from src.lcr_dataengineering_sql.db_utils import (
    create_table_if_not_exists_from_df,
    bulk_insert_df,
)

# =========================
# CONFIGURE AQUI 👇
CSV_PATH      = r"C:\deprojects\lcr-dataengineering-sql\data\raw\hr_mock.csv"   # caminho do CSV
SCHEMA        = "dbo"                              # schema de destino
TABLE         = "TB_PESSOA"                         # tabela de destino
SEP           = ","                                # separador do CSV
ENCODING      = "utf-8"                            # encoding
DECIMAL       = "."                                # separador decimal ("," em pt-BR, "." default)
CHUNKSIZE     = 100_000                            # tamanho do lote para insert
PK_COLUMNS    = None                         # ou None se não tiver PK
PARSE_DATES   = []                  # ou [] / None se não houver colunas de data
# =========================

def main():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(CSV_PATH)

    # 1º lote (usado para criar a tabela e já inserir)
    df = pd.read_csv(
        CSV_PATH,
        sep=SEP,
        encoding=ENCODING,
        decimal=DECIMAL,
        parse_dates=PARSE_DATES if PARSE_DATES else None,
        nrows=CHUNKSIZE,
        low_memory=False,
    )

    created = create_table_if_not_exists_from_df(
        df, schema=SCHEMA, table=TABLE, pk=PK_COLUMNS
    )
    if created:
        print(f"Criada tabela [{SCHEMA}].[{TABLE}]")

    # insere o primeiro lote
    rows_total = 0
    if len(df) > 0:
        bulk_insert_df(df, schema=SCHEMA, table=TABLE, chunksize=CHUNKSIZE)
        rows_total += len(df)
        print(f"Inserted first batch: {len(df)} rows")

    # processa o restante em chunks
    if CHUNKSIZE and CHUNKSIZE > 0:
        it = pd.read_csv(
            CSV_PATH,
            sep=SEP,
            encoding=ENCODING,
            decimal=DECIMAL,
            parse_dates=PARSE_DATES if PARSE_DATES else None,
            chunksize=CHUNKSIZE,
            skiprows=range(1, rows_total + 1),  # pula header + o que já leu
            low_memory=False,
        )
        for chunk in it:
            if len(chunk) == 0:
                continue
            bulk_insert_df(chunk, schema=SCHEMA, table=TABLE, chunksize=CHUNKSIZE)
            rows_total += len(chunk)
            print(f"Inserted running total: {rows_total} rows")

    print(f"Done. Total inserted: {rows_total} rows into [{SCHEMA}].[{TABLE}]")

if __name__ == "__main__":
    main()
