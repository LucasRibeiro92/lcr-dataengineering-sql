# code/repo_demo.py
from lcr_dataengineering_sql.container import db
from lcr_dataengineering_sql.features.repo import Repo

r = Repo(db)

# schema & tabela
r.ensure_schema("app")
r.create_table_raw("""
CREATE TABLE app.Clientes (
  Id INT IDENTITY(1,1) PRIMARY KEY,
  Nome NVARCHAR(200) NOT NULL
)
""")
print("count:", r.count("app", "Clientes"))

# insert CSV (assumindo colunas compatíveis)
# r.create_table_from_csv(r"C:\...\clientes.csv", schema="app", table="Clientes")
# r.insert_csv(r"C:\...\clientes.csv", schema="app", table="Clientes")

# view
r.create_view("app", "vwClientes", "SELECT Id, Nome FROM app.Clientes")
print(r.select_view("app", "vwClientes", n=5))

# procedure
r.create_procedure("app", "usp_Clientes_Top",
                   """
                   @n INT = 10
                   AS
                   BEGIN
                     SET NOCOUNT ON;
                     SELECT TOP (@n) Id, Nome FROM app.Clientes ORDER BY Id DESC;
                   END
                   """)
print(r.exec_procedure("app", "usp_Clientes_Top", {"@n": 3}))