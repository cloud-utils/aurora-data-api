import os, sys, json, unittest, logging, uuid, decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))  # noqa

import aurora_data_api
from aurora_data_api.mysql_error_codes import MySQLErrorCodes

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
                cur.execute("""
                    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
                    DROP TABLE IF EXISTS aurora_data_api_test;
                    CREATE TABLE aurora_data_api_test (
                        id SERIAL,
                        name TEXT,
                        doc JSONB DEFAULT '{}',
                        num NUMERIC (10, 5) DEFAULT 0.0
                    )
                """)
                cur.executemany("INSERT INTO aurora_data_api_test(name, doc, num) VALUES (:name, CAST(:doc AS JSONB), :num)", [{
                    "name": "row{}".format(i),
                    "doc": json.dumps({"x": i, "y": str(i), "z": [i, i * i, i ** i if i < 512 else 0]}),
                    "num": decimal.Decimal("%d.%d" % (i, i))
                } for i in range(2048)])
            except aurora_data_api.DatabaseError as e:
                if e.args[0] != MySQLErrorCodes.ER_PARSE_ERROR:
                    raise
                cls.using_mysql = True
                cur.execute("DROP TABLE IF EXISTS aurora_data_api_test")
                cur.execute("CREATE TABLE aurora_data_api_test (id SERIAL, name TEXT, birthday DATE, num NUMERIC(10, 5))")
                cur.executemany("INSERT INTO aurora_data_api_test(name, birthday, num) VALUES (:name, :birthday, :num)", [{
                    "name": "row{}".format(i),
                    "birthday": "2000-01-01",
                    "num": decimal.Decimal("%d.%d" % (i, i))
                } for i in range(2048)])

    @classmethod
    def tearDownClass(cls):
        with aurora_data_api.connect(database=cls.db_name) as conn, conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS aurora_data_api_test")

    def test_invalid_statements(self):
        with aurora_data_api.connect(database=self.db_name) as conn, conn.cursor() as cur:
            with self.assertRaisesRegex(aurora_data_api.DatabaseError, "syntax"):
                cur.execute("selec * from table")

    def test_iterators(self):
        with aurora_data_api.connect(database=self.db_name) as conn, conn.cursor() as cur:
            if not self.using_mysql:
                cur.execute("select count(*) from aurora_data_api_test where pg_column_size(doc) < :s", dict(s=2**6))
                self.assertEqual(cur.fetchone()[0], 0)
                cur.execute("select count(*) from aurora_data_api_test where pg_column_size(doc) < :s", dict(s=2**7))
                self.assertEqual(cur.fetchone()[0], 1594)
                cur.execute("select count(*) from aurora_data_api_test where pg_column_size(doc) < :s", dict(s=2**8))
                self.assertEqual(cur.fetchone()[0], 1697)
                cur.execute("select count(*) from aurora_data_api_test where pg_column_size(doc) < :s", dict(s=2**10))
                self.assertEqual(cur.fetchone()[0], 2048)

            with conn.cursor() as cursor:
                expect_row0 = (1, 'row0', '2000-01-01' if self.using_mysql else '{"x": 0, "y": "0", "z": [0, 0, 1]}', decimal.Decimal(0))
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
                self.assertEqual(data[-1][1], 'row2047')
                if not self.using_mysql:
                    self.assertEqual(json.loads(data[-1][2]), {"x": 2047, "y": str(2047), "z": [2047, 2047 * 2047, 0]})
                self.assertEqual(data[-1][-1], decimal.Decimal("2047.2047"))
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
            with self.assertRaisesRegex(conn._client.exceptions.BadRequestException,
                                        "Database response exceeded size limit"):
                cur.fetchall()

    def test_rowcount(self):
        with aurora_data_api.connect(database=self.db_name) as conn, conn.cursor() as cur:
            cur.execute("select * from aurora_data_api_test limit 8")
            self.assertEqual(cur.rowcount, 8)

        with aurora_data_api.connect(database=self.db_name) as conn, conn.cursor() as cur:
            cur.execute("select * from aurora_data_api_test limit 9000")
            self.assertEqual(cur.rowcount, 2048 if self.using_mysql else -1)

        if self.using_mysql:
            return

        with aurora_data_api.connect(database=self.db_name) as conn, conn.cursor() as cur:
            cur.executemany("INSERT INTO aurora_data_api_test(name, doc) VALUES (:name, CAST(:doc AS JSONB))", [{
                "name": "rowcount{}".format(i),
                "doc": json.dumps({"x": i, "y": str(i), "z": [i, i * i, i ** i if i < 512 else 0]})
            } for i in range(8)])

            cur.execute("UPDATE aurora_data_api_test SET doc = '{}' WHERE name like 'rowcount%'")
            self.assertEqual(cur.rowcount, 8)

            cur.execute("DELETE FROM aurora_data_api_test WHERE name like 'rowcount%'")
            self.assertEqual(cur.rowcount, 8)


if __name__ == "__main__":
    unittest.main()
