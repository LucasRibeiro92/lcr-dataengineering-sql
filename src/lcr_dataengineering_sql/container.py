# src/lcr_dataengineering_sql/container.py
from .engine import default_engine_provider
from .infra.sqlalchemy_db import SqlAlchemyDb

db = SqlAlchemyDb(engine_provider=default_engine_provider)
