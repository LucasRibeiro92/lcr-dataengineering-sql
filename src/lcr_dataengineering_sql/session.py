from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker, Session
from .engine import get_engine

_SessionLocal = None

def _get_session_factory():
    global _SessionLocal
    if _SessionLocal is None:
        engine = get_engine()
        _SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return _SessionLocal

@contextmanager
def get_session() -> Session:
    SessionLocal = _get_session_factory()
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
