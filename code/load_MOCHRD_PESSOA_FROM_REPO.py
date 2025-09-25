# code/load_MOCHRD_PESSOA_FROM_REPO.py
import os
from lcr_dataengineering_sql.container import db
from lcr_dataengineering_sql.features.repo import Repo

# =========================
# CONFIGURE AQUI 👇
CSV_PATH      = r"C:\deprojects\lcr-dataengineering-sql\data\raw\hr_mock.csv"   # caminho do CSV
SCHEMA        = "MOCKED_HR_DATA"                              # schema de destino
TABLE         = "MOCHRD_PESSOA_FROM_REPO"                         # tabela de destino
COLUMN_PREFIX = "MOCHRD_"
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

    r = Repo(db)

    r.insert_csv_with_prefix(CSV_PATH, SCHEMA, TABLE, COLUMN_PREFIX)

if __name__ == "__main__":
    main()
