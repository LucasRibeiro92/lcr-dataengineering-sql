# code/multi_db_demo.py
from lcr_dataengineering_sql.container_multi import db_router
from lcr_dataengineering_sql.features.repo_router import RepoRouter

def main():
    rr = RepoRouter(db_router)

    '''
    # MSSQL
    r_sql = rr.for_db("MSSQL")
    r_sql.ensure_schema("MULTI_DB")
    r_sql.create_table_raw("CREATE TABLE MULTI_DB.TESTE (id INT PRIMARY KEY, nome NVARCHAR(100));")
    '''
    # Postgres
    r_pg = rr.for_db("PG")
    r_pg.ensure_schema("multi_db")
    r_pg.create_table_raw('CREATE TABLE multi_db."teste" (id INT PRIMARY KEY, nome VARCHAR(100));')

    # MySQL (schema=database; use o nome do database na URL)
    r_my = rr.for_db("MYSQL")
    r_my.create_table_raw("CREATE TABLE multi_db_teste (id INT PRIMARY KEY, nome VARCHAR(100));")

if __name__ == "__main__":
    main()