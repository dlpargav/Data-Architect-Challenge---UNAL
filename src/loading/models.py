"""
SQLAlchemy ORM models — Star schema (Bronze / Silver / Gold).

Schema overview:
  bronze.*  — raw ingested data, 1:1 copy of source files with metadata
  silver.*  — cleaned, filtered, typed data (no aggregation yet)
  gold.*    — star schema: dimensions + fact table, KPIs pre-computed

Design decisions:
  - Three PostgreSQL schemas provide clear data layer separation and enable
    schema-level access control (e.g., Tableau user only needs SELECT on gold.*).
  - Upsert (INSERT ... ON CONFLICT DO UPDATE) ensures idempotency —
    re-running the pipeline never duplicates data.
  - 'fecha_procesamiento' on fact table provides traceability without a
    separate audit table.
"""

from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase

# ─── Schema-aware base classes ────────────────────────────────────────────────

class BronzeBase(DeclarativeBase):
    metadata = MetaData(schema="bronze")

class SilverBase(DeclarativeBase):
    metadata = MetaData(schema="silver")

class GoldBase(DeclarativeBase):
    metadata = MetaData(schema="gold")


# ─── Bronze layer ─────────────────────────────────────────────────────────────

class BronzeDocentes(BronzeBase):
    """Raw docentes data — exact copy of SNIES source."""
    __tablename__ = "docentes_raw"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    codigo_de_la_institucion = Column(Integer, nullable=True)
    institucion = Column(Text, nullable=True)
    municipio_de_domicilio_de_la_ies = Column(Text, nullable=True)
    numero_de_docentes = Column(Integer, nullable=True)
    semestre = Column(SmallInteger, nullable=True)
    ano = Column(SmallInteger, nullable=False)
    ingestion_timestamp = Column(Text, nullable=False)


class BronzeEstudiantes(BronzeBase):
    """Raw estudiantes data — exact copy of SNIES source."""
    __tablename__ = "estudiantes_raw"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    codigo_de_la_institucion = Column(Integer, nullable=True)
    institucion = Column(Text, nullable=True)
    municipio_de_domicilio_de_la_ies = Column(Text, nullable=True)
    numero_de_estudiantes_matriculados = Column(Integer, nullable=True)
    semestre = Column(SmallInteger, nullable=True)
    ano = Column(SmallInteger, nullable=False)
    ingestion_timestamp = Column(Text, nullable=False)


# ─── Gold layer — Star Schema ──────────────────────────────────────────────────

class DimInstitucion(GoldBase):
    """
    Dimension: Higher Education Institution (IES).

    Grain: one row per institution (slowly-changing — sector/SUE flag treated
    as current state; historical SCD-2 is out of scope for this project scale).
    """
    __tablename__ = "dim_institucion"

    codigo_institucion = Column(Integer, primary_key=True)
    nombre_institucion = Column(String(500), nullable=False)
    sector = Column(String(50), nullable=True)       # Oficial / Privado
    caracter = Column(String(100), nullable=True)    # Universidad, Inst. Tecnológica…
    municipio_domicilio = Column(String(200), nullable=True)
    departamento_domicilio = Column(String(200), nullable=True)
    es_sue = Column(Boolean, nullable=False, default=False)


class DimPeriodo(GoldBase):
    """
    Dimension: Academic period.

    Grain: one row per (year [× semester]). Semester is NULL for annual data.
    """
    __tablename__ = "dim_periodo"
    __table_args__ = (
        UniqueConstraint("ano", "semestre", name="uq_periodo_ano_semestre"),
    )

    id_periodo = Column(Integer, primary_key=True, autoincrement=True)
    ano = Column(SmallInteger, nullable=False)
    semestre = Column(SmallInteger, nullable=True)   # NULL = annual granularity


class FactCapacidadAcademica(GoldBase):
    """
    Fact: Academic capacity KPI per institution per period.

    Grain: one row per (institution × period).
    Pre-computed KPI enables fast Tableau queries without runtime division.
    """
    __tablename__ = "fact_capacidad_academica"
    __table_args__ = (
        UniqueConstraint(
            "codigo_institucion", "id_periodo",
            name="uq_fact_inst_periodo"
        ),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    codigo_institucion = Column(Integer, nullable=False)    # FK → dim_institucion
    id_periodo = Column(Integer, nullable=False)             # FK → dim_periodo
    total_estudiantes_matriculados = Column(BigInteger, nullable=True)
    total_docentes = Column(Integer, nullable=True)
    relacion_estudiantes_por_docente = Column(Numeric(8, 2), nullable=True)
    fecha_procesamiento = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
