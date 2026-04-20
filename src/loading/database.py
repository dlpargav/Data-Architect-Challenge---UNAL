"""
Database engine, session management, and schema initialization.

Usage:
    from src.loading.database import get_engine, init_db

    engine = get_engine()
    init_db(engine)   # idempotent — safe to call on every pipeline run
"""

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.utils.config import DATABASE_URL
from src.loading.models import BronzeBase, GoldBase

logger = logging.getLogger(__name__)

_engine: Engine | None = None


def get_engine() -> Engine:
    """Return a singleton SQLAlchemy engine."""
    global _engine
    if _engine is None:
        _engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,   # validates connection before use
            pool_size=5,
            max_overflow=10,
        )
        logger.info("Database engine created: %s", DATABASE_URL.split("@")[-1])
    return _engine


def get_session() -> Session:
    """Return a new SQLAlchemy session."""
    SessionLocal = sessionmaker(bind=get_engine())
    return SessionLocal()


def init_db(engine: Engine | None = None) -> None:
    """
    Create all schemas and tables if they don't already exist.
    Safe to call on every pipeline run (idempotent via CREATE IF NOT EXISTS).
    """
    engine = engine or get_engine()

    # Create PostgreSQL schemas first (SQLAlchemy DDL doesn't do this)
    with engine.connect() as conn:
        for schema in ("bronze", "silver", "gold"):
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            logger.info("Schema '%s' ensured.", schema)
        conn.commit()

    # Create all ORM-mapped tables
    BronzeBase.metadata.create_all(engine)
    GoldBase.metadata.create_all(engine)
    logger.info("All tables created/verified.")
