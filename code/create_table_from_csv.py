import os
import pandas as pd
from lcr_dataengineering_sql.db_utils import (
    create_table_if_not_exists_from_df,
)

# =========================
# CONFIGURE AQUI 👇
CSV_PATH      = r"C:\deprojects\lcr-dataengineering-sql\data\raw\hr_mock.csv" # caminho do CSV
SCHEMA        = "dbo" # schema de destino
TABLE         = "TB_PESSOA_TREATED" # tabela de destino
COLUMN_PREFIX = "HRMKPS_" # prefixo das colunas
SEP           = "," # separador do CSV
ENCODING      = "utf-8" # encoding
DECIMAL       = "."  # separador decimal ("," em pt-BR, "." default)
CHUNKSIZE     = 100 # tamanho do lote para insert
PK_COLUMNS    = None # ou None se não tiver PK
PARSE_DATES   = [] # ou [] / None se não houver colunas de data
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
        df, schema=SCHEMA, table=TABLE, column_prefix = COLUMN_PREFIX, pk=PK_COLUMNS
    )
    if created:
        print(f"Criada tabela [{SCHEMA}].[{TABLE}]")


if __name__ == "__main__":
    main()