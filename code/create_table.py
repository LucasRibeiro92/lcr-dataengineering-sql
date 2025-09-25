from lcr_dataengineering_sql.container import db
from lcr_dataengineering_sql.features.repo import Repo

r = Repo(db)

SQL = """
IF OBJECT_ID('dbo.TESTE','U') IS NULL
BEGIN
    CREATE TABLE dbo.TESTE (
        id INT IDENTITY(1,1) PRIMARY KEY,
        nome NVARCHAR(100) NOT NULL
    );
END
"""

if __name__ == "__main__":
    try:
        r.create_table_raw(SQL)
        print("Table created successfully")
    except:
        print("An exception occurred")