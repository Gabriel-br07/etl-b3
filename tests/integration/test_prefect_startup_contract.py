from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent.parent
COMPOSE = ROOT / "compose.yaml"
ENTRYPOINT = ROOT / "docker" / "entrypoint.sh"
COTAHIST_ENTRYPOINT = ROOT / "docker" / "entrypoint_cotahist.sh"


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


def test_compose_cotahist_uses_dedicated_entrypoint():
    content = COMPOSE.read_text(encoding="utf-8")
    assert "cotahist:" in content
    assert 'entrypoint: ["/app/docker/entrypoint_cotahist.sh"]' in content


def test_cotahist_entrypoint_does_not_run_alembic():
    content = COTAHIST_ENTRYPOINT.read_text(encoding="utf-8")
    assert "alembic upgrade head" not in content
    assert "setpriv --reuid=1001 --regid=1001 --clear-groups -- \"$@\"" in content


def test_scheduler_and_cotahist_share_explicit_image_tag():
    content = COMPOSE.read_text(encoding="utf-8")
    assert "scheduler:" in content
    assert "cotahist:" in content
    assert "image: etlb3_scheduler:local" in content
