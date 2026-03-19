# Run tests and optional lint/typecheck. Usage: ./scripts/run_tests.ps1 [-Unit|-Integration|-E2e|-Coverage|-Lint|-Typecheck|-Precommit]
param(
    [switch] $Unit,
    [switch] $Integration,
    [switch] $E2e,
    [switch] $Coverage,
    [switch] $Lint,
    [switch] $Typecheck,
    [switch] $Precommit
)

$ErrorActionPreference = "Stop"
Set-Location (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

if ($Lint) {
    uv run ruff check .
} elseif ($Typecheck) {
    uv run ty check
} elseif ($Precommit) {
    uv run pre-commit run --all-files
} elseif ($Unit) {
    uv run pytest tests/unit -q
} elseif ($Integration) {
    uv run pytest tests/integration -m "not db" --tb=short
} elseif ($E2e) {
    uv run pytest tests/e2e -m e2e -v
} elseif ($Coverage) {
    uv run pytest tests/ -m "not e2e and not live and not db" --cov --cov-report=term-missing --tb=short
} else {
    uv run pytest tests/ -m "not e2e and not live and not db" --tb=short
}
