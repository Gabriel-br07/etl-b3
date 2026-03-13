# Copilot Code Review Instructions

This repository contains a Python ETL pipeline that ingests market data from B3
and stores it in PostgreSQL / TimescaleDB.

The architecture includes:

- Scrapers (Playwright)
- Data parsing (CSV and JSONL)
- Polars transforms
- SQLAlchemy repositories
- ETL pipelines
- pytest test suite

When reviewing code, prioritize correctness, robustness, and data integrity.

---

# General Review Guidelines

When reviewing code changes:

1. Identify logical errors and edge cases.
2. Detect race conditions and concurrency risks.
3. Verify that functions handle invalid inputs safely.
4. Ensure logging is informative and not excessively noisy.
5. Verify that new code follows the project's architecture patterns.
6. Avoid duplicated logic — prefer reusable helpers.

Focus on correctness before style.

---

# ETL-Specific Review Rules

This repository processes financial market data.

During review:

- Validate schema compatibility with database models.
- Ensure transformations preserve numeric precision (Decimal).
- Prevent duplicate inserts in fact tables.
- Check that pipelines remain idempotent.
- Ensure timestamps and dates are handled correctly.

Never allow silent data corruption.

---

# Database Rules

Database interactions must follow these rules:

- Use SQLAlchemy ORM models.
- Do not bypass repository layer.
- Avoid raw SQL unless necessary.
- Ensure transactions are handled correctly.
- Prevent duplicate rows through unique constraints or upserts.

TimescaleDB hypertables must not break primary key constraints.

---

# Testing Rules

Tests must follow Test-Driven Development practices.

Ensure:

- Tests reflect real function signatures.
- Mock data types match production types.
- Tests use pytest fixtures where possible.
- Each test validates a single behavior.

Avoid:

- brittle tests
- tests depending on implementation details
- duplicate tests

Important types:

- timestamps → datetime
- numeric values → Decimal
- dataframes → Polars DataFrame

---

# Code Quality

Prefer:

- small functions
- clear naming
- explicit typing
- module-level loggers

Avoid:

- duplicated transformations
- large monolithic functions
- unnecessary abstractions

---

# Performance Considerations

The ETL may process millions of rows.

Avoid:

- expensive operations inside loops
- repeated parsing of the same file
- unnecessary dataframe conversions

Prefer vectorized Polars operations.

---

# Security and Reliability

Ensure:

- file reads are safe
- ETL jobs are idempotent
- errors are logged clearly
- pipelines fail loudly instead of silently skipping errors

---

# Pull Request Review Priorities

When reviewing a PR, prioritize:

1. data correctness
2. pipeline stability
3. database integrity
4. test coverage
5. performance
6. code style
