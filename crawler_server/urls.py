"""
Database storing info on URLs
"""
import os
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from psycopg2 import connect
from psycopg2.extras import execute_values


class URLState(Enum):
    """
    URL state update is idempotent and can only progress forwards.
    """
    NEW = 0         # One user has identified this URL
    CONFIRMED = 1   # A different user has identified the same URL
    ASSIGNED = 2    # The crawler has given the URL to a user to crawl
    CRAWLED = 3     # At least one user has crawled the URL


@dataclass
class URLStatus:
    url: str
    state: URLState
    user_id_hash: str
    updated: datetime


def get_connection():
    return connect(user=os.environ["USERNAME"])


def create_tables(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS urls (
        url VARCHAR PRIMARY KEY,
        status INT NOT NULL DEFAULT 0,
        user_id_hash VARCHAR NOT NULL,
        updated TIMESTAMP NOT NULL DEFAULT NOW()
    )
    """

    with conn.cursor() as cursor:
        cursor.execute(sql)


def update_url_status(conn, url_statuses: list[URLStatus]):
    sql = """
    INSERT INTO urls (url, status, user_id_hash) values %s
    ON CONFLICT (url) DO UPDATE SET status=excluded.status, user_id_hash=excluded.user_id_hash
    """

    data = [(status.url, status.state.value, status.user_id_hash) for status in url_statuses]

    with conn.cursor() as cursor:
        execute_values(cursor, sql, data)


def user_found_urls(user_id_hash: str, urls: list[str]):
    sql = f"""
    INSERT INTO urls (url, status, user_id_hash, updated) values %s
    ON CONFLICT (url) DO UPDATE SET 
      status = CASE
        WHEN excluded.status={URLState.NEW.value} AND excluded.user_id_hash != urls.user_id_hash THEN {URLState.CONFIRMED.value}
        ELSE {URLState.NEW.value}
      END,
      user_id_hash=excluded.user_id_hash,
      updated=excluded.updated
    """

    now = datetime.now()
    data = [(url, URLState.NEW.value, user_id_hash, now) for url in urls]

    with conn.cursor() as cursor:
        execute_values(cursor, sql, data)


if __name__ == "__main__":
    with get_connection() as conn:
        create_tables(conn)
        # update_url_status(conn, [URLStatus("https://mwmbl.org", URLState.NEW, "test-user", datetime.now())])
        user_found_urls("Test user", ["a", "b", "c"])
        user_found_urls("Another user", ["b", "c", "d"])