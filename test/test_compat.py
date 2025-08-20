import logging
import os
import sys
import unittest
import aurora_data_api
from base import CoreAuroraDataAPITest, PEP249ConformanceTestMixin

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

logging.basicConfig(level=logging.INFO)
logging.getLogger("aurora_data_api").setLevel(logging.DEBUG)
logging.getLogger("urllib3.connectionpool").setLevel(logging.DEBUG)


class TestAuroraDataAPIConformance(PEP249ConformanceTestMixin, CoreAuroraDataAPITest):
    """Conformance test class for synchronous tests. Sets up connection to run tests."""

    driver = aurora_data_api
    connection = None

    def setUp(self):
        """Setup mock objects for connection and cursor as member variables."""
        self.connection = self.driver.connect(
            database=self.db_name, aurora_cluster_arn=self.cluster_arn, secret_arn=self.secret_arn
        )

    def test_cursor_interface(self):
        """[Cursor] Runs formal interface checks on the cursor."""
        with self.connection.cursor() as cur:
            self._assert_cursor_interface(cur)


# Remove these classes to avoid instantiation errors
del CoreAuroraDataAPITest


if __name__ == "__main__":
    unittest.main()
