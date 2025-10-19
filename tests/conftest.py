import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from endolla_watcher import storage

TEST_DB_URL = os.getenv("ENDOLLA_TEST_DB_URL")


@pytest.fixture(scope="module")
def db_url():
    if not TEST_DB_URL:
        pytest.skip("ENDOLLA_TEST_DB_URL not configured", allow_module_level=True)
    return TEST_DB_URL


@pytest.fixture
def conn(db_url):
    connection = storage.connect(db_url)
    with connection.cursor() as cur:
        cur.execute("DELETE FROM station_fingerprint_heatmap")
        cur.execute("DELETE FROM station_fingerprint_jobs")
        cur.execute("DELETE FROM port_status")
    connection.commit()
    yield connection
    connection.close()
