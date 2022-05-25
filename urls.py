"""
Database storing info on URLs
"""
import os

from psycopg2 import connect


def get_connection():
    return connect(os.environ['DB_URL'])


def create_tables():
    sql = """
    CREATE TABLE IF NOT EXISTS urls (
        url STRING PRIMARY KEY,
        status INT NOT NULL,
        updated TIMESTAMP NOT NULL DEFAULT NOW()
    )
    """

    conn = get_connection()
    conn.execute(sql)


