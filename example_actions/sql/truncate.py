from lcr_dataengineering_sql.container import db
from lcr_dataengineering_sql.features.repo import Repo

if __name__ == "__main__":
    r = Repo(db)
    r.truncate_table("MOCKED_HR_DATA", "MOCHRD_PESSOA_FROM_REPO")