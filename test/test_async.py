import asyncio
import datetime
import decimal
import json
import logging
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import aurora_data_api.async_ as async_   # noqa
from aurora_data_api.error_codes_mysql import MySQLErrorCodes  # noqa
from aurora_data_api.error_codes_postgresql import PostgreSQLErrorCodes  # noqa

logging.basicConfig(level=logging.INFO)
logging.getLogger("aurora_data_api").setLevel(logging.DEBUG)
logging.getLogger("urllib3.connectionpool").setLevel(logging.DEBUG)


class AsyncTestCase(unittest.TestCase):
    """Base class for async test cases."""
    
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
    def tearDown(self):
        self.loop.close()
        
    def async_test(coro):
        """Decorator to run async test methods."""
        def wrapper(self):
            return self.loop.run_until_complete(coro(self))
        return wrapper


class TestAuroraDataAPI(AsyncTestCase):
    using_mysql = False

    @classmethod
    def setUpClass(cls):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(cls._async_setUpClass())
        finally:
            loop.close()
    
    @classmethod
    async def _async_setUpClass(cls):
        cls.db_name = os.environ.get("AURORA_DB_NAME", __name__)
        cls.cluster_arn = os.environ.get("AURORA_CLUSTER_ARN")
        cls.secret_arn = os.environ.get("SECRET_ARN")
        async with await async_.connect(database=cls.db_name, aurora_cluster_arn=cls.cluster_arn, secret_arn=cls.secret_arn) as conn:
            cur = await conn.cursor()
            try:
                await cur.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
                await cur.execute("DROP TABLE IF EXISTS aurora_data_api_test")
                await cur.execute(
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
                await cur.executemany(
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
            except async_.MySQLError.ER_PARSE_ERROR:
                cls.using_mysql = True
                await cur.execute("DROP TABLE IF EXISTS aurora_data_api_test")
                await cur.execute(
                    "CREATE TABLE aurora_data_api_test "
                    "(id SERIAL, name TEXT, birthday DATE, num NUMERIC(10, 5), ts TIMESTAMP)"
                )
                await cur.executemany(
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
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(cls._async_tearDownClass())
        finally:
            loop.close()
            
    @classmethod
    async def _async_tearDownClass(cls):
        async with await async_.connect(database=cls.db_name, aurora_cluster_arn=cls.cluster_arn, secret_arn=cls.secret_arn) as conn:
            cur = await conn.cursor()
            await cur.execute("DROP TABLE IF EXISTS aurora_data_api_test")

    @AsyncTestCase.async_test
    async def test_invalid_statements(self):
        async with await async_.connect(database=self.db_name, aurora_cluster_arn=self.cluster_arn, secret_arn=self.secret_arn) as conn:
            cur = await conn.cursor()
            with self.assertRaises(
                (async_.exceptions.PostgreSQLError.ER_SYNTAX_ERR, async_.MySQLError.ER_PARSE_ERROR)
            ):
                await cur.execute("selec * from table")

    @AsyncTestCase.async_test
    async def test_iterators(self):
        async with await async_.connect(database=self.db_name, aurora_cluster_arn=self.cluster_arn, secret_arn=self.secret_arn) as conn:
            cur = await conn.cursor()
            if not self.using_mysql:
                await cur.execute("select count(*) from aurora_data_api_test where pg_column_size(doc) < :s", dict(s=2**6))
                result = await cur.fetchone()
                self.assertEqual(result[0], 0)
                await cur.execute("select count(*) from aurora_data_api_test where pg_column_size(doc) < :s", dict(s=2**7))
                result = await cur.fetchone()
                self.assertEqual(result[0], 1977)
                await cur.execute("select count(*) from aurora_data_api_test where pg_column_size(doc) < :s", dict(s=2**8))
                result = await cur.fetchone()
                self.assertEqual(result[0], 2048)
                await cur.execute("select count(*) from aurora_data_api_test where pg_column_size(doc) < :s", dict(s=2**10))
                result = await cur.fetchone()
                self.assertEqual(result[0], 2048)

            cursor = await conn.cursor()
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
            await cursor.execute("select * from aurora_data_api_test")
            async for f in cursor:
                if i == 0:
                    self.assertEqual(f, expect_row0)
                i += 1
            self.assertEqual(i, 2048)

            await cursor.execute("select * from aurora_data_api_test")
            data = await cursor.fetchall()
            self.assertEqual(data[0], expect_row0)
            self.assertEqual(data[-1][0], 2048)
            self.assertEqual(data[-1][1], "row2047")
            if not self.using_mysql:
                self.assertEqual(json.loads(data[-1][2]), {"x": 2047, "y": str(2047), "z": [2047, 2047 * 2047, 0]})
            self.assertEqual(data[-1][-2], decimal.Decimal("2047.2047"))
            self.assertEqual(len(data), 2048)
            self.assertEqual(len(await cursor.fetchall()), 0)

            await cursor.execute("select * from aurora_data_api_test")
            i = 0
            while True:
                result = await cursor.fetchone()
                if not result:
                    break
                i += 1
            self.assertEqual(i, 2048)

            await cursor.execute("select * from aurora_data_api_test")
            while True:
                fm = await cursor.fetchmany(1001)
                if not fm:
                    break
                self.assertIn(len(fm), [1001, 46])

    @unittest.skip(
        "This test now fails because the API was changed to terminate and delete the transaction when the "
        "data returned by the statement exceeds the limit, making automated recovery impossible."
    )
    @AsyncTestCase.async_test
    async def test_pagination_backoff(self):
        if self.using_mysql:
            return
        async with await async_.connect(database=self.db_name, aurora_cluster_arn=self.cluster_arn, secret_arn=self.secret_arn) as conn:
            cur = await conn.cursor()
            sql_template = "select concat({}) from aurora_data_api_test"
            sql = sql_template.format(", ".join(["cast(doc as text)"] * 64))
            await cur.execute(sql)
            result = await cur.fetchall()
            self.assertEqual(len(result), 2048)

            concat_args = ", ".join(["cast(doc as text)"] * 100)
            sql = sql_template.format(", ".join("concat({})".format(concat_args) for i in range(32)))
            await cur.execute(sql)
            with self.assertRaisesRegex(
                Exception, "Database response exceeded size limit"
            ):
                await cur.fetchall()

    @AsyncTestCase.async_test
    async def test_postgres_exceptions(self):
        if self.using_mysql:
            return
        async with await async_.connect(database=self.db_name, aurora_cluster_arn=self.cluster_arn, secret_arn=self.secret_arn) as conn:
            cur = await conn.cursor()
            table = "aurora_data_api_nonexistent_test_table"
            with self.assertRaises(async_.exceptions.PostgreSQLError.ER_UNDEF_TABLE) as e:
                sql = f"select * from {table}"
                await cur.execute(sql)
            self.assertTrue(f'relation "{table}" does not exist' in str(e.exception))
            self.assertTrue(isinstance(e.exception.response, dict))

    @AsyncTestCase.async_test
    async def test_rowcount(self):
        async with await async_.connect(database=self.db_name, aurora_cluster_arn=self.cluster_arn, secret_arn=self.secret_arn) as conn:
            cur = await conn.cursor()
            await cur.execute("select * from aurora_data_api_test limit 8")
            self.assertEqual(cur.rowcount, 8)

        async with await async_.connect(database=self.db_name, aurora_cluster_arn=self.cluster_arn, secret_arn=self.secret_arn) as conn:
            cur = await conn.cursor()
            await cur.execute("select * from aurora_data_api_test limit 9000")
            self.assertEqual(cur.rowcount, 2048)

        if self.using_mysql:
            return

        async with await async_.connect(database=self.db_name, aurora_cluster_arn=self.cluster_arn, secret_arn=self.secret_arn) as conn:
            cur = await conn.cursor()
            await cur.executemany(
                "INSERT INTO aurora_data_api_test(name, doc) VALUES (:name, CAST(:doc AS JSONB))",
                [
                    {
                        "name": "rowcount{}".format(i),
                        "doc": json.dumps({"x": i, "y": str(i), "z": [i, i * i, i**i if i < 512 else 0]}),
                    }
                    for i in range(8)
                ],
            )

            await cur.execute("UPDATE aurora_data_api_test SET doc = '{}' WHERE name like 'rowcount%'")
            self.assertEqual(cur.rowcount, 8)

            await cur.execute("DELETE FROM aurora_data_api_test WHERE name like 'rowcount%'")
            self.assertEqual(cur.rowcount, 8)

    @AsyncTestCase.async_test
    async def test_continue_after_timeout(self):
        if os.environ.get("TEST_CONTINUE_AFTER_TIMEOUT", "False") != "True":
            self.skipTest("TEST_CONTINUE_AFTER_TIMEOUT env var is not 'True'")

        if self.using_mysql:
            self.skipTest("Not implemented for MySQL")

        try:
            async with await async_.connect(database=self.db_name, aurora_cluster_arn=self.cluster_arn, secret_arn=self.secret_arn) as conn:
                cur = await conn.cursor()
                with self.assertRaisesRegex(Exception, "StatementTimeoutException"):
                    await cur.execute(
                        (
                            "INSERT INTO aurora_data_api_test(name) SELECT 'continue_after_timeout'"
                            "FROM (SELECT pg_sleep(50)) q"
                        )
                    )
                with self.assertRaisesRegex(async_.DatabaseError, "current transaction is aborted"):
                    await cur.execute("SELECT COUNT(*) FROM aurora_data_api_test WHERE name = 'continue_after_timeout'")

            async with await async_.connect(database=self.db_name, aurora_cluster_arn=self.cluster_arn, secret_arn=self.secret_arn) as conn:
                cur = await conn.cursor()
                await cur.execute("SELECT COUNT(*) FROM aurora_data_api_test WHERE name = 'continue_after_timeout'")
                result = await cur.fetchone()
                self.assertEqual(result, (0,))

            async with await async_.connect(
                database=self.db_name, continue_after_timeout=True,
                aurora_cluster_arn=self.cluster_arn, secret_arn=self.secret_arn
            ) as conn:
                cur = await conn.cursor()
                with self.assertRaisesRegex(Exception, "StatementTimeoutException"):
                    await cur.execute(
                        (
                            "INSERT INTO aurora_data_api_test(name) SELECT 'continue_after_timeout' "
                            "FROM (SELECT pg_sleep(50)) q"
                        )
                    )
                await cur.execute("SELECT COUNT(*) FROM aurora_data_api_test WHERE name = 'continue_after_timeout'")
                result = await cur.fetchone()
                self.assertEqual(result, (1,))
        finally:
            async with await async_.connect(database=self.db_name, aurora_cluster_arn=self.cluster_arn, secret_arn=self.secret_arn) as conn:
                cur = await conn.cursor()
                await cur.execute("DELETE FROM aurora_data_api_test WHERE name = 'continue_after_timeout'")


if __name__ == "__main__":
    unittest.main()
