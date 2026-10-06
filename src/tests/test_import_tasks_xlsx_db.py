"""--apply against a THROWAWAY userdb (opt-in; skipped by default).

    URI=$(../dev/testdb.sh proto06_test_crawl_import)
    # orgs the map needs, with matching ids (the importer maps theme → org *id*):
    psql ... -c "INSERT INTO orgs (id, public_id, name, contact_email) OVERRIDING SYSTEM VALUE
                 SELECT i, 'test-org-'||i, 'test org '||i, 'test@example.invalid'
                 FROM unnest(ARRAY[47,62,91,92,93,101,102,105,106]) i ON CONFLICT DO NOTHING"
    IMPORT_TASKS_TEST_DB_URI=$URI python -m pytest src/tests/test_import_tasks_xlsx_db.py -v

Refuses any database whose name lacks ``_test``. Leaves the imported rows in
place (the DB is throwaway); re-running is fine because the import is idempotent.
"""
from __future__ import annotations

import json
import os
import shutil
from urllib.parse import urlparse

import pytest

from src.scripts import import_tasks_xlsx as imp

URI = os.environ.get("IMPORT_TASKS_TEST_DB_URI", "")
pytestmark = pytest.mark.skipif(not URI, reason="IMPORT_TASKS_TEST_DB_URI not set")


@pytest.fixture(scope="module")
def conn():
    psycopg2 = pytest.importorskip("psycopg2")
    assert "_test" in urlparse(URI).path, f"refusing non-test database {URI!r}"
    c = psycopg2.connect(URI)
    c.autocommit = True
    yield c
    c.close()


def _count(conn, where="true"):
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM crawl_tasks WHERE {where}")
        return cur.fetchone()[0]


def test_apply_is_idempotent_and_matches_plan(conn, tmp_path, capsys):
    rows = imp.read_xlsx(imp.DEFAULT_XLSX)
    with conn.cursor() as cur:
        before = imp.fetch_db_state(cur)
    planned = imp.plan_rows(rows, before["actor_classes"], before["legacy"], before["live"], before["orgs"])
    to_insert = [p for p in planned if p.action == "insert"]
    n0 = _count(conn)

    # customers dir: a copy of the real ones' yaml + ids.json (never the real folders)
    for cdir in imp.DEFAULT_CUSTOMERS_DIR.iterdir() if imp.DEFAULT_CUSTOMERS_DIR.is_dir() else []:
        if (cdir / "customer.yaml").exists():
            (tmp_path / cdir.name).mkdir()
            for f in ("customer.yaml", "ids.json"):
                if (cdir / f).exists():
                    shutil.copy(cdir / f, tmp_path / cdir.name / f)

    imp.main(["--db-uri", URI, "--apply", "--write-ids", "--customers-dir", str(tmp_path)])
    out1 = capsys.readouterr().out
    assert f"inserted {len(to_insert)} rows" in out1
    assert _count(conn) == n0 + len(to_insert)

    # second apply: everything already there
    imp.main(["--db-uri", URI, "--apply"])
    out2 = capsys.readouterr().out
    assert "inserted 0 rows" in out2
    assert _count(conn) == n0 + len(to_insert)

    # dry run touches nothing and reports 0 to insert
    imp.main(["--db-uri", URI])
    assert "DRY RUN: 0 rows to insert" in capsys.readouterr().out

    # DB constraints held: retired rows carry retired_at; params stored sorted
    assert _count(conn, "status = 'retired' AND retired_at IS NULL") == 0
    assert _count(conn, "search_params <> crawl_params_key(search_params)") == 0
    assert _count(conn, "legacy_task_id = '145' AND status = 'retired'") == 1
    assert _count(conn, "legacy_theme = 'orizaba'") == 0
    assert _count(conn, "legacy_task_id IN ('35~2','36~2','37~2')") == 3

    # ids.json written into the tmp copy only, for themes that were imported
    zita = tmp_path / "zitacuaro" / "ids.json"
    if zita.exists():
        db_ids = json.loads(zita.read_text())["crawl_task_db_ids"]
        assert set(db_ids) == {str(i) for i in range(146, 159)}
    oriz = tmp_path / "orizaba" / "ids.json"
    if oriz.exists():
        assert "crawl_task_db_ids" not in json.loads(oriz.read_text())


def test_live_unique_index_backs_the_plan(conn):
    """The DB itself rejects a second live row with the same order-insensitive key."""
    import psycopg2

    with conn.cursor() as cur:
        cur.execute("SELECT org_id, actor_class, search_params FROM crawl_tasks "
                    "WHERE status = 'active' AND jsonb_array_length(search_params) > 1 LIMIT 1")
        row = cur.fetchone()
        if row is None:
            pytest.skip("no multi-param active row")
        org, actor, params = row
        with pytest.raises(psycopg2.errors.UniqueViolation):
            cur.execute("INSERT INTO crawl_tasks (org_id, actor_class, search_params, status) "
                        "VALUES (%s, %s, %s::jsonb, 'paused')",
                        (org, actor, json.dumps(list(reversed(params)))))
