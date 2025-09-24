from src.lcr_dataengineering_sql.db_utils import exec_sql

SQL = """
IF OBJECT_ID('dbo.exemplo','U') IS NULL
BEGIN
    CREATE TABLE dbo.exemplo (
        id INT IDENTITY(1,1) PRIMARY KEY,
        nome NVARCHAR(100) NOT NULL
    );
END
"""

if __name__ == "__main__":
    exec_sql(SQL)
    print("Tabela dbo.exemplo pronta.")