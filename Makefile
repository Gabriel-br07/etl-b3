# Convenience targets (Unix/macOS/Git Bash). On Windows, use README PowerShell examples.
.PHONY: test test-unit test-integration test-e2e test-coverage lint typecheck precommit

lint:
	uv run ruff check .

typecheck:
	uv run ty check

precommit:
	uv run pre-commit run --all-files

test:
	uv run pytest tests/ -m "not e2e and not live and not db" --tb=short

test-unit:
	uv run pytest tests/unit -q

test-integration:
	uv run pytest tests/integration -m "not db" --tb=short

test-e2e:
	uv run pytest tests/e2e -m e2e -v

test-coverage:
	uv run pytest tests/ -m "not e2e and not live and not db" --cov --cov-report=term-missing
