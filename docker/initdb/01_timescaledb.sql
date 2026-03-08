-- =============================================================================
-- docker/initdb/01_timescaledb.sql
-- Executed automatically by the PostgreSQL Docker image on first boot.
--
-- Creates the TimescaleDB extension in the etlb3 database.
-- The hypertable itself is created by the Alembic migration so that it is
-- version-controlled and repeatable.
-- =============================================================================

-- Enable TimescaleDB (idempotent – safe to re-run)
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

