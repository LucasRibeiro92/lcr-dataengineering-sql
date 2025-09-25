from lcr_dataengineering_sql.container import db
from lcr_dataengineering_sql.features.repo import Repo

if __name__ == "__main__":
    r = Repo(db)
    print("count:", r.count("dbo", "TB_PESSOA"))
    print(r.select_top("dbo", "TB_PESSOA", n=5))
    #rows = query_all("SELECT TOP (1) * FROM dbo.exemplo")
    #for r in rows:
    #    print(r.id, r.nome)