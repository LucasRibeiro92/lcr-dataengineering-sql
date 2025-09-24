# code/query_generic.py
import pandas as pd
from lcr_dataengineering_sql.container import db
from lcr_dataengineering_sql.features.repo import Repo

if __name__ == "__main__":
    repo = Repo(db, default_schema="dbo", default_table="TB_PESSOA")

    print("Qtd:", repo.count())
    top5 = repo.listar_top(5)
    print(top5)
    '''
    # inserir lote (usa defaults)
    df = pd.DataFrame([{"Employee_Name": "Foo", "EmpID": 999}])  # ajuste colunas conforme sua tabela
    repo.inserir_lote(df)
    '''