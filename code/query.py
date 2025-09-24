from src.lcr_dataengineering_sql.db_utils import query_all

if __name__ == "__main__":
    rows = query_all("SELECT TOP (1) * FROM dbo.exemplo")
    for r in rows:
        print(r.id, r.nome)