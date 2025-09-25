# code/create_table_from_csv.py
import os
from lcr_dataengineering_sql.container import db
from lcr_dataengineering_sql.features.repo import Repo

# ===== CONFIG =====
CSV_PATH      = r"/data/raw/hr_mock.csv"
SCHEMA        = "MOCKED_HR_DATA"
TABLE         = "MOCHRD_PESSOA_FROM_REPO"
COLUMN_PREFIX = "MOCHRD_"
SEP           = ","
ENCODING      = "utf-8"
DECIMAL       = "."
PARSE_DATES   = []          # ex.: ["DateofHire","DateofTermination"]
SAMPLE_ROWS   = 100_000     # quantas linhas usar p/ criar a tabela (inferência de tipos)
CHUNKSIZE     = 100_000     # tamanho dos lotes de insert
PK_COLUMNS    = None        # ex.: ["HRMKPS_ID"]
# ===================

def main():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(CSV_PATH)

    r = Repo(db)

    # schema & tabela
    r.ensure_schema("MOCKED_HR_DATA")

    created = r.create_table_from_csv_with_prefix(
        csv_path=CSV_PATH,
        schema=SCHEMA,
        table=TABLE,
        column_prefix=COLUMN_PREFIX,
        pk=PK_COLUMNS,
        sep=SEP,
        encoding=ENCODING,
        decimal=DECIMAL,
        parse_dates=PARSE_DATES,
        sample_rows=SAMPLE_ROWS,
        chunksize_insert=CHUNKSIZE,
    )
    print("Tabela criada agora?", created)
    print(f"Concluído: CSV → [{SCHEMA}].[{TABLE}] (prefixo: {COLUMN_PREFIX})")

if __name__ == "__main__":
    main()