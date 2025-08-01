"""
Base test classes for Aurora Data API async and sync tests
"""

import datetime
import decimal
import json
import logging
import os
import sys
import unittest
from abc import ABC, abstractmethod

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from aurora_data_api.error_codes_mysql import MySQLErrorCodes  # noqa
from aurora_data_api.error_codes_postgresql import PostgreSQLErrorCodes  # noqa

logging.basicConfig(level=logging.INFO)
logging.getLogger("aurora_data_api").setLevel(logging.DEBUG)
logging.getLogger("urllib3.connectionpool").setLevel(logging.DEBUG)


# @unittest.skip
class CoreAuroraDataAPITest(unittest.TestCase):
    """Base of base class to provide configuration for Aurora Data API tests"""

    @classmethod
    def setUpClass(cls):
        cls.db_name = os.environ.get("AURORA_DB_NAME", __name__)
        cls.cluster_arn = os.environ.get("AURORA_CLUSTER_ARN")
        cls.secret_arn = os.environ.get("SECRET_ARN")


# @unittest.skip
class BaseAuroraDataAPITest(CoreAuroraDataAPITest, ABC):
    """Base test class for Aurora Data API tests (both sync and async)"""

    using_mysql = False

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_test_data()

    @classmethod
    @abstractmethod
    def _setup_test_data(cls):
        """Abstract method to set up test data - implemented differently for sync/async"""
        pass

    @classmethod
    @abstractmethod
    def _teardown_test_data(cls):
        """Abstract method to tear down test data - implemented differently for sync/async"""
        pass

    @classmethod
    def tearDownClass(cls):
        cls._teardown_test_data()

    @abstractmethod
    def test_invalid_statements(self):
        """Test invalid SQL statements"""
        pass

    @abstractmethod
    def test_iterators(self):
        """Test cursor iteration functionality"""
        pass

    @abstractmethod
    def test_postgres_exceptions(self):
        """Test PostgreSQL-specific exceptions"""
        pass

    @abstractmethod
    def test_rowcount(self):
        """Test rowcount functionality"""
        pass

    @abstractmethod
    def test_continue_after_timeout(self):
        """Test continue after timeout functionality"""
        pass

    def get_expected_row0(self):
        """Get the expected first row for testing"""
        return (
            1,
            "row0",
            # Note: data api v1 used JSON serialization with extra whitespace; v2 uses compact serialization
            datetime.date(2000, 1, 1) if self.using_mysql else '{"x":0,"y":"0","z":[0,0,1]}',
            decimal.Decimal(0.0),
            datetime.datetime(2020, 9, 17, 13, 49, 33)
            if self.using_mysql
            else datetime.datetime(2020, 9, 17, 13, 49, 32, 780180),
        )

    def get_test_data(self):
        """Get test data for insertion"""
        if self.using_mysql:
            return [
                {
                    "name": "row{}".format(i),
                    "birthday": "2000-01-01",
                    "num": decimal.Decimal("%d.%d" % (i, i)),
                    "ts": "2020-09-17 13:49:32.780180",
                }
                for i in range(2048)
            ]
        else:
            return [
                {
                    "name": "row{}".format(i),
                    # Note: data api v1 supports up to 512**512 but v2 only supports up to 128**128
                    "doc": json.dumps({"x": i, "y": str(i), "z": [i, i * i, i**i if i < 128 else 0]}),
                    "num": decimal.Decimal("%d.%d" % (i, i)),
                    "ts": "2020-09-17 13:49:32.780180",
                }
                for i in range(2048)
            ]

    def get_postgresql_create_table_sql(self):
        """Get PostgreSQL CREATE TABLE statement"""
        return """
            CREATE TABLE aurora_data_api_test (
                id SERIAL,
                name TEXT,
                doc JSONB DEFAULT '{}',
                num NUMERIC (10, 5) DEFAULT 0.0,
                ts TIMESTAMP WITHOUT TIME ZONE
            )
        """

    def get_mysql_create_table_sql(self):
        """Get MySQL CREATE TABLE statement"""
        return (
            "CREATE TABLE aurora_data_api_test (id SERIAL, name TEXT, birthday DATE, num NUMERIC(10, 5), ts TIMESTAMP)"
        )

    def get_postgresql_insert_sql(self):
        """Get PostgreSQL INSERT statement"""
        return """
            INSERT INTO aurora_data_api_test(name, doc, num, ts)
            VALUES (:name, CAST(:doc AS JSONB), :num, CAST(:ts AS TIMESTAMP))
        """

    def get_mysql_insert_sql(self):
        """Get MySQL INSERT statement"""
        return (
            "INSERT INTO aurora_data_api_test(name, birthday, num, ts) VALUES "
            "(:name, :birthday, :num, CAST(:ts AS DATETIME))"
        )


class PEP249ConformanceTestMixin:
    """
    A mixin containing a comprehensive set of formal conformance tests
    for PEP 249 v2.0, based on the full specification.
    Reference: https://peps.python.org/pep-0249/
    """

    # Helper methods to perform assertions
    def _assert_callable(self, obj, attr_name):
        self.assertTrue(hasattr(obj, attr_name), f"'{attr_name}' must exist")
        self.assertTrue(callable(getattr(obj, attr_name)), f"'{attr_name}' must be callable")

    def _assert_attribute(self, obj, attr_name):
        self.assertTrue(hasattr(obj, attr_name), f"Attribute '{attr_name}' must exist")

    def _assert_optional_callable(self, obj, attr_name):
        if hasattr(obj, attr_name):
            self.assertTrue(callable(getattr(obj, attr_name)), f"If '{attr_name}' exists, it must be callable")

    # === [Module Interface] Tests ===

    def test_module_globals_and_constructor(self):
        """[Module] Tests for globals (apilevel, etc.) and the connect() constructor."""
        self._assert_callable(self.driver, "connect")
        self._assert_attribute(self.driver, "apilevel")
        self.assertEqual(self.driver.apilevel, "2.0", "apilevel must be '2.0' for this specification")
        self._assert_attribute(self.driver, "threadsafety")
        self.assertIn(self.driver.threadsafety, [0, 1, 2, 3])
        self._assert_attribute(self.driver, "paramstyle")
        self.assertIn(self.driver.paramstyle, ["qmark", "numeric", "named", "format", "pyformat"])

    def test_module_exceptions(self):
        """[Module] Tests for the existence and hierarchy of all required exception classes."""
        exceptions = [
            "Warning",
            "Error",
            "InterfaceError",
            "DatabaseError",
            "DataError",
            "OperationalError",
            "IntegrityError",
            "InternalError",
            "ProgrammingError",
            "NotSupportedError",
        ]
        for exc_name in exceptions:
            self._assert_attribute(self.driver, exc_name)
        self.assertTrue(issubclass(self.driver.Warning, Exception))
        self.assertTrue(issubclass(self.driver.Error, Exception))
        self.assertTrue(issubclass(self.driver.InterfaceError, self.driver.Error))
        self.assertTrue(issubclass(self.driver.DatabaseError, self.driver.Error))
        self.assertTrue(issubclass(self.driver.DataError, self.driver.DatabaseError))
        self.assertTrue(issubclass(self.driver.OperationalError, self.driver.DatabaseError))
        self.assertTrue(issubclass(self.driver.IntegrityError, self.driver.DatabaseError))
        self.assertTrue(issubclass(self.driver.InternalError, self.driver.DatabaseError))
        self.assertTrue(issubclass(self.driver.ProgrammingError, self.driver.DatabaseError))
        self.assertTrue(issubclass(self.driver.NotSupportedError, self.driver.DatabaseError))

    def test_module_type_objects_and_constructors(self):
        """[Module] Tests for the existence of all required Type Objects and Constructors."""
        for constructor in [
            "Date",
            "Time",
            "Timestamp",
            "DateFromTicks",
            "TimeFromTicks",
            "TimestampFromTicks",
            "Binary",
        ]:
            self._assert_callable(self.driver, constructor)
        for type_obj in ["STRING", "BINARY", "NUMBER", "DATETIME", "ROWID"]:
            self._assert_attribute(self.driver, type_obj)

    # === [Connection and Cursor] Tests and Assertion Helpers ===
    def test_connection_interface(self):
        """[Connection] Tests to assert the complete interface of a Connection object."""
        self._assert_callable(self.connection, "close")
        self._assert_callable(self.connection, "commit")
        self._assert_callable(self.connection, "cursor")
        self._assert_optional_callable(self.connection, "rollback")

    def _assert_cursor_interface(self, cursor):
        """[Cursor] Helper to assert the complete interface of a Cursor object."""
        self._assert_attribute(cursor, "description")
        self._assert_attribute(cursor, "rowcount")
        self._assert_attribute(cursor, "arraysize")
        self._assert_callable(cursor, "close")
        self._assert_callable(cursor, "execute")
        self._assert_callable(cursor, "executemany")
        self._assert_callable(cursor, "fetchone")
        self._assert_callable(cursor, "fetchall")
        self._assert_callable(cursor, "fetchmany")
        self._assert_callable(cursor, "setinputsizes")
        self._assert_callable(cursor, "setoutputsize")
        self._assert_optional_callable(cursor, "callproc")
        self._assert_optional_callable(cursor, "nextset")
