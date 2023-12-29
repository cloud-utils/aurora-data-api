import datetime
import decimal
import json
import logging
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import aurora_data_api  # noqa
from aurora_data_api.mysql_error_codes import MySQLErrorCodes  # noqa
from aurora_data_api.postgresql_error_codes import PostgreSQLErrorCodes  # noqa

logging.basicConfig(level=logging.INFO)
logging.getLogger("aurora_data_api").setLevel(logging.DEBUG)
logging.getLogger("urllib3.connectionpool").setLevel(logging.DEBUG)


class TestAuroraDataAPI(unittest.TestCase):
    using_mysql = False

    @classmethod
    def setUpClass(cls):
        cls.db_name = os.environ.get("AURORA_DB_NAME", __name__)
        with aurora_data_api.connect(database=cls.db_name) as conn, conn.cursor() as cur:
            try:
                cur.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
                cur.execute('DROP TABLE IF EXISTS aurora_data_api_test')
                cur.execute(
                    """
                    CREATE TABLE aurora_data_api_test (
                        id SERIAL,
                        name TEXT,
                        doc JSONB DEFAULT '{}',
                        num NUMERIC (10, 5) DEFAULT 0.0,
                        ts TIMESTAMP WITHOUT TIME ZONE
                    )
                    """
                )
                cur.executemany(
                    """
                    INSERT INTO aurora_data_api_test(name, doc, num, ts)
                    VALUES (:name, CAST(:doc AS JSONB), :num, CAST(:ts AS TIMESTAMP))
                """,
                    [
                        {
                            "name": "row{}".format(i),
                            # Note: data api v1 supports up to 512**512 but v2 only supports up to 128**128
                            "doc": json.dumps({"x": i, "y": str(i), "z": [i, i * i, i**i if i < 128 else 0]}),
                            "num": decimal.Decimal("%d.%d" % (i, i)),
                            "ts": "2020-09-17 13:49:32.780180",
                        }
                        for i in range(2048)
                    ],
                )
            except aurora_data_api.MySQLError.ER_PARSE_ERROR:
                cls.using_mysql = True
                cur.execute("DROP TABLE IF EXISTS aurora_data_api_test")
                cur.execute(
                    "CREATE TABLE aurora_data_api_test "
                    "(id SERIAL, name TEXT, birthday DATE, num NUMERIC(10, 5), ts TIMESTAMP)"
                )
                cur.executemany(
                    (
                        "INSERT INTO aurora_data_api_test(name, birthday, num, ts) VALUES "
                        "(:name, :birthday, :num, CAST(:ts AS DATETIME))"
                    ),
                    [
                        {
                            "name": "row{}".format(i),
                            "birthday": "2000-01-01",
                            "num": decimal.Decimal("%d.%d" % (i, i)),
                            "ts": "2020-09-17 13:49:32.780180",
                        }
                        for i in range(2048)
                    ],
                )

    @classmethod
    def tearDownClass(cls):
        with aurora_data_api.connect(database=cls.db_name) as conn, conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS aurora_data_api_test")

    def test_invalid_statements(self):
        with aurora_data_api.connect(database=self.db_name) as conn, conn.cursor() as cur:
            with self.assertRaises(
                (aurora_data_api.exceptions.PostgreSQLError.ER_SYNTAX_ERR, aurora_data_api.MySQLError.ER_PARSE_ERROR)
            ):
                cur.execute("selec * from table")

    def test_iterators(self):
        with aurora_data_api.connect(database=self.db_name) as conn, conn.cursor() as cur:
            if not self.using_mysql:
                cur.execute("select count(*) from aurora_data_api_test where pg_column_size(doc) < :s", dict(s=2**6))
                self.assertEqual(cur.fetchone()[0], 0)
                cur.execute("select count(*) from aurora_data_api_test where pg_column_size(doc) < :s", dict(s=2**7))
                self.assertEqual(cur.fetchone()[0], 1977)
                cur.execute("select count(*) from aurora_data_api_test where pg_column_size(doc) < :s", dict(s=2**8))
                self.assertEqual(cur.fetchone()[0], 2048)
                cur.execute("select count(*) from aurora_data_api_test where pg_column_size(doc) < :s", dict(s=2**10))
                self.assertEqual(cur.fetchone()[0], 2048)

            with conn.cursor() as cursor:
                expect_row0 = (
                    1,
                    "row0",
                    # Note: data api v1 used JSON serialization with extra whitespace; v2 uses compact serialization
                    datetime.date(2000, 1, 1) if self.using_mysql else '{"x":0,"y":"0","z":[0,0,1]}',
                    decimal.Decimal(0.0),
                    datetime.datetime(2020, 9, 17, 13, 49, 33)
                    if self.using_mysql
                    else datetime.datetime(2020, 9, 17, 13, 49, 32, 780180),
                )
                i = 0
                cursor.execute("select * from aurora_data_api_test")
                for f in cursor:
                    if i == 0:
                        self.assertEqual(f, expect_row0)
                    i += 1
                self.assertEqual(i, 2048)

                cursor.execute("select * from aurora_data_api_test")
                data = cursor.fetchall()
                self.assertEqual(data[0], expect_row0)
                self.assertEqual(data[-1][0], 2048)
                self.assertEqual(data[-1][1], "row2047")
                if not self.using_mysql:
                    self.assertEqual(json.loads(data[-1][2]), {"x": 2047, "y": str(2047), "z": [2047, 2047 * 2047, 0]})
                self.assertEqual(data[-1][-2], decimal.Decimal("2047.2047"))
                self.assertEqual(len(data), 2048)
                self.assertEqual(len(cursor.fetchall()), 0)

                cursor.execute("select * from aurora_data_api_test")
                i = 0
                while True:
                    if not cursor.fetchone():
                        break
                    i += 1
                self.assertEqual(i, 2048)

                cursor.execute("select * from aurora_data_api_test")
                while True:
                    fm = cursor.fetchmany(1001)
                    if not fm:
                        break
                    self.assertIn(len(fm), [1001, 46])

    @unittest.skip(
        "This test now fails because the API was changed to terminate and delete the transaction when the "
        "data returned by the statement exceeds the limit, making automated recovery impossible."
    )
    def test_pagination_backoff(self):
        if self.using_mysql:
            return
        with aurora_data_api.connect(database=self.db_name) as conn, conn.cursor() as cur:
            sql_template = "select concat({}) from aurora_data_api_test"
            sql = sql_template.format(", ".join(["cast(doc as text)"] * 64))
            cur.execute(sql)
            self.assertEqual(len(cur.fetchall()), 2048)

            concat_args = ", ".join(["cast(doc as text)"] * 100)
            sql = sql_template.format(", ".join("concat({})".format(concat_args) for i in range(32)))
            cur.execute(sql)
            with self.assertRaisesRegex(
                conn._client.exceptions.BadRequestException, "Database response exceeded size limit"
            ):
                cur.fetchall()

    def test_postgres_exceptions(self):
        if self.using_mysql:
            return
        with aurora_data_api.connect(database=self.db_name) as conn, conn.cursor() as cur:
            table = "aurora_data_api_nonexistent_test_table"
            with self.assertRaises(aurora_data_api.exceptions.PostgreSQLError.ER_UNDEF_TABLE) as e:
                sql = f"select * from {table}"
                cur.execute(sql)
            self.assertTrue(f'relation "{table}" does not exist' in str(e.exception))
            self.assertTrue(isinstance(e.exception.response, dict))

    def test_rowcount(self):
        with aurora_data_api.connect(database=self.db_name) as conn, conn.cursor() as cur:
            cur.execute("select * from aurora_data_api_test limit 8")
            self.assertEqual(cur.rowcount, 8)

        with aurora_data_api.connect(database=self.db_name) as conn, conn.cursor() as cur:
            cur.execute("select * from aurora_data_api_test limit 9000")
            self.assertEqual(cur.rowcount, 2048)

        if self.using_mysql:
            return

        with aurora_data_api.connect(database=self.db_name) as conn, conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO aurora_data_api_test(name, doc) VALUES (:name, CAST(:doc AS JSONB))",
                [
                    {
                        "name": "rowcount{}".format(i),
                        "doc": json.dumps({"x": i, "y": str(i), "z": [i, i * i, i**i if i < 512 else 0]}),
                    }
                    for i in range(8)
                ],
            )

            cur.execute("UPDATE aurora_data_api_test SET doc = '{}' WHERE name like 'rowcount%'")
            self.assertEqual(cur.rowcount, 8)

            cur.execute("DELETE FROM aurora_data_api_test WHERE name like 'rowcount%'")
            self.assertEqual(cur.rowcount, 8)

    def test_continue_after_timeout(self):
        if os.environ.get("TEST_CONTINUE_AFTER_TIMEOUT", "False") != "True":
            self.skipTest("TEST_CONTINUE_AFTER_TIMEOUT env var is not 'True'")

        if self.using_mysql:
            self.skipTest("Not implemented for MySQL")

        try:
            with aurora_data_api.connect(database=self.db_name) as conn, conn.cursor() as cur:
                with self.assertRaisesRegex(conn._client.exceptions.ClientError, "StatementTimeoutException"):
                    cur.execute(
                        (
                            "INSERT INTO aurora_data_api_test(name) SELECT 'continue_after_timeout'"
                            "FROM (SELECT pg_sleep(50)) q"
                        )
                    )
                with self.assertRaisesRegex(aurora_data_api.DatabaseError, "current transaction is aborted"):
                    cur.execute("SELECT COUNT(*) FROM aurora_data_api_test WHERE name = 'continue_after_timeout'")

            with aurora_data_api.connect(database=self.db_name) as conn, conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM aurora_data_api_test WHERE name = 'continue_after_timeout'")
                self.assertEqual(cur.fetchone(), (0,))

            with aurora_data_api.connect(
                database=self.db_name, continue_after_timeout=True
            ) as conn, conn.cursor() as cur:
                with self.assertRaisesRegex(conn._client.exceptions.ClientError, "StatementTimeoutException"):
                    cur.execute(
                        (
                            "INSERT INTO aurora_data_api_test(name) SELECT 'continue_after_timeout' "
                            "FROM (SELECT pg_sleep(50)) q"
                        )
                    )
                cur.execute("SELECT COUNT(*) FROM aurora_data_api_test WHERE name = 'continue_after_timeout'")
                self.assertEqual(cur.fetchone(), (1,))
        finally:
            with aurora_data_api.connect(database=self.db_name) as conn, conn.cursor() as cur:
                cur.execute("DELETE FROM aurora_data_api_test WHERE name = 'continue_after_timeout'")


if __name__ == "__main__":
    unittest.main()
