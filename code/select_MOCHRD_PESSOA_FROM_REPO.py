from lcr_dataengineering_sql.container import db
from lcr_dataengineering_sql.features.repo import Repo

if __name__ == "__main__":
    r = Repo(db)
    print("count:", r.count("MOCKED_HR_DATA", "MOCHRD_PESSOA_FROM_REPO"))
    print(r.select_top("MOCKED_HR_DATA", "MOCHRD_PESSOA_FROM_REPO", n=5))