# src/lcr_dataengineering_sql/container_multi.py
from __future__ import annotations
from typing import Callable, Dict
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from .config_multi import get_url, list_aliases

def _engine_kwargs_for_url(url: str) -> dict:
    # Só MSSQL+pyodbc suporta fast_executemany no create_engine
    if url.lower().startswith("mssql+pyodbc://"):
        return {"fast_executemany": True}
    return {}

def _engine_provider_from_url(url: str) -> Callable[[], Engine]:
    eng: Engine | None = None
    def _provider() -> Engine:
        nonlocal eng
        if eng is None:
            kwargs = _engine_kwargs_for_url(url)
            eng = create_engine(url, pool_pre_ping=True, future=True, **kwargs)
        return eng
    return _provider

def build_db_router(aliases: list[str] | None = None) -> Dict[str, object]:
    aliases = aliases or list_aliases()
    router = {}
    for alias in aliases:
        url = get_url(alias)
        provider = _engine_provider_from_url(url)
        router[alias] = provider
    # embrulha providers em SqlAlchemyDb
    from .infra.sqlalchemy_db import SqlAlchemyDb
    return {alias: SqlAlchemyDb(provider) for alias, provider in router.items()}

# router padrão
db_router = build_db_router()
