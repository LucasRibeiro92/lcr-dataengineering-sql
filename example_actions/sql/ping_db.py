# code/ping_db.py
from lcr_dataengineering_sql.container_multi import db_router  # mapeia alias -> Db
# se você usa apenas um banco, pode importar de container.py e ignorar o router

# Escolha aqui o alias:
ALIAS = "MSSQL"   # opções típicas: "MSSQL", "PG", "MYSQL"

if __name__ == "__main__":
    db = db_router[ALIAS]   # pega o Db daquele alias
    ok = db.ping()
    print(f"Ping {ALIAS}: {'OK' if ok else 'FALHOU'}")