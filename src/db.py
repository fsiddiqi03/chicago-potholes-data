"""Database connection helper. Used by all loaders."""
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg2
from psycopg2.extensions import connection as Connection

from .config import DATABASE_URL


@contextmanager
def get_connection() -> Iterator[Connection]:
    """
    Yields a Postgres connection. Auto-commits on success, rolls back on
    exception, always closes. Use as: `with get_connection() as conn: ...`
    """
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
