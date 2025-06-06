from psycopg2 import connect
from psycopg2.extras import RealDictCursor, RealDictRow
from psycopg2 import Error
from typing import Optional, Union, Any, Tuple
from config import env_setting


class DataBaseConnection:
    def __init__(self):
        self.connection = None
        self.connect()
        print("=" * 20)
        print("db connected successfully ")
        print("=" * 20)

    def connect(self):
        try:
            self.connection = connect(database=env_setting.POSTGRES_DB, user=env_setting.POSTGRES_USER,
                                      password=env_setting.POSTGRES_PASSWORD, host="127.0.0.1", port=5432)
        except Error as e:
            print(f"Error connecting to PostgreSQL DB: {e}")

    def fetch_query_all(self, query: str, params: Optional[Union[list[Any], Tuple[Any, ...]]] = None) -> list[
                                                                                                             RealDictRow] | None:
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as conn:
                conn.execute(query, params)
                return conn.fetchall()
        except Exception as e:
            print("Fetching failed:", e)
            return None

    def fetch_query_once(self, query: str,
                         params: Optional[Union[list[Any], Tuple[Any, ...]]] = None) -> RealDictRow | None:

        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as conn:
                conn.execute(query, params)
                return conn.fetchone()
        except Exception as e:
            print("Fetching failed:", e)
            return None

    def close(self):
        self.connection.close()
        print("=" * 10)
        print("Connection closed")
        print("=" * 10)
