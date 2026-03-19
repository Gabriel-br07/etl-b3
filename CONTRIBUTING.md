# Contributing

## Local quality gate (required before commit)

Install dev dependencies and enable Git hooks once per clone:

```powershell
uv sync --locked --extra dev
uv run pre-commit install
```

On each `git commit`, [pre-commit](https://pre-commit.com/) runs:

- [Ruff](https://docs.astral.sh/ruff/) (`ruff check`)
- [ty](https://docs.astral.sh/ty/) (`ty check`) on `app`, `scripts`, and `docker` (see `[tool.ty.src]` in `pyproject.toml`)

Run the same checks manually:

```powershell
uv run ruff check .
uv run ty check
```

Or run all hooks without committing:

```powershell
uv run pre-commit run --all-files
```

**Shortcuts:** `make lint`, `make typecheck`, `make precommit` (Unix/Git Bash), or `.\scripts\run_tests.ps1 -Lint` / `-Typecheck` / `-Precommit` on Windows.

**Windows note:** Git must be able to find `uv` on `PATH` when hooks run (same environment you use in the terminal).

## Tests

See [README.md](README.md) § *Running tests* and [TESTING-STRATEGY.md](TESTING-STRATEGY.md).

## CI

GitHub Actions workflow [`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs Ruff, ty, and pytest on push/PR to `main` / `master`.
