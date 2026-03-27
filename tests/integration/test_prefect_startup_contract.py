from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent.parent
COMPOSE = ROOT / "compose.yaml"
ENTRYPOINT = ROOT / "docker" / "entrypoint.sh"


def test_compose_scheduler_waits_for_db_health():
    content = COMPOSE.read_text(encoding="utf-8")
    assert "depends_on:" in content
    assert "condition: service_healthy" in content


def test_entrypoint_runs_migrations_before_starting_runtime():
    content = ENTRYPOINT.read_text(encoding="utf-8")
    non_comment_lines = [
        line.strip()
        for line in content.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    migration_lines = [line for line in non_comment_lines if "alembic upgrade head" in line]
    assert migration_lines, "entrypoint must execute alembic upgrade head before runtime"
    assert "setpriv --reuid=1001 --regid=1001 --clear-groups -- \"$@\"" in content
    assert content.index(migration_lines[0]) < content.index(
        "setpriv --reuid=1001 --regid=1001 --clear-groups -- \"$@\""
    )
