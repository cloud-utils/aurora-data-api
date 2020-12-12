Changes for v0.2.7 (2020-12-12)
===============================

-  Catch ValueError when timestamp returns 5 decimal places instead of 6
   (#22)

Changes for v0.2.6 (2020-11-13)
===============================

Update handling of error codes and response size error for latest Data
API version

Changes for v0.2.5 (2020-10-03)
===============================

Fix refactoring of timestamp casting

Changes for v0.2.4 (2020-10-02)
===============================

Fix bug in param prep hint application

Changes for v0.2.3 (2020-10-02)
===============================

-  Fix strptime handling in Python 3.6 and earlier

-  Fix type hint handling for arrays

Changes for v0.2.2 (2020-10-02)
===============================

-  Fix type hints and tests

Changes for v0.2.1 (2020-10-01)
===============================

-  connect(): Accept and ignore port kwarg

-  Add numeric and decmial support (#16)

-  Accept boto3 rds data client as a connection arg to engine options
   (#10)

Changes for v0.2.0 (2020-01-02)
===============================

-  Begin MySQL support

Changes for v0.1.3 (2019-12-14)
===============================

-  Add rowcount support for update/delete.

Changes for v0.1.2 (2019-11-10)
===============================

-  Add lastrowid support; get database error on MySQL

-  Reset current response at execute start



Changes for v0.1.0 (2019-10-29)
===============================

-  Process nulls correctly

-  Parameterize page size decrement

-  Implement cursor.scroll()

-  Test and documentation improvements

Changes for v0.0.5 (2019-10-21)
===============================

-  Enable autopagination, implement rowcount

Changes for v0.0.4 (2019-10-17)
===============================

-  Implement pagination and executemany

Changes for v0.0.4 (2019-10-17)
===============================

-  Implement pagination and executemany

Changes for v0.0.3 (2019-10-10)
===============================

-  Fix fetchone on empty results

Changes for v0.0.2 (2019-10-10)
===============================

-  Update documentation

Changes for v0.0.1 (2019-10-10)
===============================

-  Begin aurora-data-api

