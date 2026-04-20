-- init.sql: executed by PostgreSQL on first container startup.
-- Creates the three medallion schemas used by the pipeline.
-- SQLAlchemy handles individual table creation via ORM DDL.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Grant privileges so the pipeline user can work across all schemas
GRANT ALL PRIVILEGES ON SCHEMA bronze TO snies;
GRANT ALL PRIVILEGES ON SCHEMA silver TO snies;
GRANT ALL PRIVILEGES ON SCHEMA gold TO snies;
