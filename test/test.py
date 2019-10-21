import os, sys, json, unittest, logging, uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))  # noqa

import aurora_data_api

logging.basicConfig(level=logging.INFO)
logging.getLogger("aurora_data_api").setLevel(logging.DEBUG)
logging.getLogger("urllib3.connectionpool").setLevel(logging.DEBUG)


class TestAuroraDataAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_name = os.environ.get("AURORA_DB_NAME", __name__)
        with aurora_data_api.connect(database=cls.db_name) as conn, conn.cursor() as cur:
            cur.execute("""
                CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
                DROP TABLE IF EXISTS aurora_data_api_test;
                CREATE TABLE aurora_data_api_test (
                    id SERIAL,
                    name TEXT,
                    doc JSONB DEFAULT '{}'
                )
            """)
            cur.executemany("INSERT INTO aurora_data_api_test(name, doc) VALUES (:name, CAST(:doc AS JSONB))", [{
                "name": "row{}".format(i),
                "doc": json.dumps({"x": i, "y": str(i), "z": [i, i * i, i ** i if i < 512 else 0]})
            } for i in range(2048)])

    def test_invalid_statements(self):
        with aurora_data_api.connect(database=self.db_name) as conn, conn.cursor() as cur:
            with self.assertRaisesRegex(conn._client.exceptions.BadRequestException, "syntax error"):
                cur.execute("selec * from table")

    def test_iterators(self):
        with aurora_data_api.connect(database=self.db_name) as conn, conn.cursor() as cur:
            cur.execute("select count(*) from aurora_data_api_test where pg_column_size(doc) < :s", dict(s=2**6))
            self.assertEqual(cur.fetchone()[0], 0)
            cur.execute("select count(*) from aurora_data_api_test where pg_column_size(doc) < :s", dict(s=2**7))
            self.assertEqual(cur.fetchone()[0], 1594)
            cur.execute("select count(*) from aurora_data_api_test where pg_column_size(doc) < :s", dict(s=2**8))
            self.assertEqual(cur.fetchone()[0], 1697)
            cur.execute("select count(*) from aurora_data_api_test where pg_column_size(doc) < :s", dict(s=2**10))
            self.assertEqual(cur.fetchone()[0], 2048)

            with conn.cursor() as cursor:
                i = 0
                cursor.execute("select * from aurora_data_api_test")
                for f in cursor:
                    if i == 0:
                        self.assertEqual(f, (1, 'row0', '{"x": 0, "y": "0", "z": [0, 0, 1]}'))
                    i += 1
                self.assertEqual(i, 2048)

                cursor.execute("select * from aurora_data_api_test")
                data = cursor.fetchall()
                self.assertEqual(data[0], (1, 'row0', '{"x": 0, "y": "0", "z": [0, 0, 1]}'))
                self.assertEqual(data[-1][0], 2048)
                self.assertEqual(data[-1][1], 'row2047')
                self.assertEqual(json.loads(data[-1][-1]), {"x": 2047, "y": str(2047), "z": [2047, 2047 * 2047, 0]})
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
        with aurora_data_api.connect(database=self.db_name) as conn, conn.cursor() as cur:
            sql = "select concat({}) from aurora_data_api_test"
            sql = sql.format(", ".join(["cast(doc as text)"] * 64))
            cur.execute(sql)
            self.assertEqual(len(cur.fetchall()), 2048)

    def test_rowcount(self):
        with aurora_data_api.connect(database=self.db_name) as conn, conn.cursor() as cur:
            cur.execute("select * from aurora_data_api_test limit 8")
            self.assertEqual(cur.rowcount, 8)


if __name__ == "__main__":
    unittest.main()
