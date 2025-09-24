from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from .config import DbConfig

def default_engine_provider() -> Engine:
    cfg = DbConfig()
    return create_engine(
        cfg.sqlalchemy_url(),
        pool_pre_ping=True,
        future=True,
        fast_executemany=True,  # bom para inserts em lote com pyodbc
    )