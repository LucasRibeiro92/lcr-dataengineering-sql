from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from .config import DbConfig

_engine: Engine | None = None

def get_engine(cfg: DbConfig | None = None) -> Engine:
    global _engine
    if _engine is None:
        cfg = cfg or DbConfig()
        _engine = create_engine(cfg.sqlalchemy_url(), pool_pre_ping=True, future=True, fast_executemany=True)
    return _engine