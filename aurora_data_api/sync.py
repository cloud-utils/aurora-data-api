"""
aurora-data-api - A Python DB-API 2.0 client for the AWS Aurora Serverless Data API
"""
import time
import random
import string
import reprlib
import threading
from .base import BaseAuroraDataAPIClient, BaseAuroraDataAPICursor, logger
from .exceptions import (
    InterfaceError,
    DatabaseError,
)
import botocore.session


class SyncAuroraDataAPIClient(BaseAuroraDataAPIClient):
    _client_init_lock = threading.Lock()

    def __init__(
        self,
        dbname=None,
        aurora_cluster_arn=None,
        secret_arn=None,
        rds_data_client=None,
        charset=None,
        continue_after_timeout=None,
    ):
        super().__init__(dbname, aurora_cluster_arn, secret_arn, rds_data_client, charset, continue_after_timeout)
        if rds_data_client is None:
            with self._client_init_lock:
                session = botocore.session.get_session()
                self._client = session.create_client("rds-data")

    def commit(self):
        if self._transaction_id:
            res = self._client.commit_transaction(
                resourceArn=self._aurora_cluster_arn, secretArn=self._secret_arn, transactionId=self._transaction_id
            )
            self._transaction_id = None
            if res["transactionStatus"] != "Transaction Committed":
                raise DatabaseError("Error while committing transaction: {}".format(res))

    def rollback(self):
        if self._transaction_id:
            self._client.rollback_transaction(
                resourceArn=self._aurora_cluster_arn, secretArn=self._secret_arn, transactionId=self._transaction_id
            )
            self._transaction_id = None

    def cursor(self):
        if self._transaction_id is None:
            res = self._client.begin_transaction(
                database=self._dbname,
                resourceArn=self._aurora_cluster_arn,
                # schema="string", TODO
                secretArn=self._secret_arn,
            )
            self._transaction_id = res["transactionId"]
        cursor = SyncAuroraDataAPICursor(
            client=self._client,
            dbname=self._dbname,
            aurora_cluster_arn=self._aurora_cluster_arn,
            secret_arn=self._secret_arn,
            transaction_id=self._transaction_id,
            continue_after_timeout=self._continue_after_timeout,
        )
        if self._charset:
            cursor.execute("SET character_set_client = '{}'".format(self._charset))
        return cursor

    def __enter__(self):
        return self

    def __exit__(self, err_type, value, traceback):
        if err_type is not None:
            self.rollback()
        else:
            self.commit()


class SyncAuroraDataAPICursor(BaseAuroraDataAPICursor):
    def __init__(
        self,
        client=None,
        dbname=None,
        aurora_cluster_arn=None,
        secret_arn=None,
        transaction_id=None,
        continue_after_timeout=None,
    ):
        super().__init__(client, dbname, aurora_cluster_arn, secret_arn, transaction_id, continue_after_timeout)

    def _start_paginated_query(self, execute_statement_args, records_per_page=None):
        # MySQL cursors are non-scrollable (https://dev.mysql.com/doc/refman/8.0/en/cursors.html)
        # - may not support page autosizing
        # - FETCH requires INTO - may need to write all results into a server side var and iterate on it
        pg_cursor_name = "{}_{}_{}".format(
            __name__, int(time.time()), "".join(random.choices(string.ascii_letters + string.digits, k=8))
        )
        cursor_stmt = "DECLARE " + pg_cursor_name + " SCROLL CURSOR FOR "
        execute_statement_args["sql"] = cursor_stmt + execute_statement_args["sql"]
        self._client.execute_statement(**execute_statement_args)
        self._paging_state = {
            "execute_statement_args": dict(execute_statement_args),
            "records_per_page": records_per_page or self.arraysize,
            "pg_cursor_name": pg_cursor_name,
        }

    def execute(self, operation, parameters=None):
        self._current_response, self._iterator, self._paging_state = None, None, None
        execute_statement_args = dict(self._prepare_execute_args(operation), includeResultMetadata=True)
        if self._continue_after_timeout is not None:
            execute_statement_args["continueAfterTimeout"] = self._continue_after_timeout
        if parameters:
            execute_statement_args["parameters"] = self._format_parameter_set(parameters)
        logger.debug("execute %s", reprlib.repr(operation.strip()))
        try:
            res = self._client.execute_statement(**execute_statement_args)
            if "columnMetadata" in res:
                self._set_description(res["columnMetadata"])
            self._current_response = self._render_response(res)
        except (self._client.exceptions.BadRequestException, self._client.exceptions.DatabaseErrorException) as e:
            if "Please paginate your query" in str(e):
                self._start_paginated_query(execute_statement_args)
            elif "Database returned more than the allowed response size limit" in str(e):
                self._start_paginated_query(execute_statement_args, records_per_page=max(1, self.arraysize // 2))
            else:
                raise self._get_database_error(e) from e
        self._iterator = iter(self)

    def executemany(self, operation, seq_of_parameters):
        logger.debug("executemany %s", reprlib.repr(operation.strip()))
        for batch in self._page_input(seq_of_parameters):
            batch_execute_statement_args = dict(
                self._prepare_execute_args(operation), parameterSets=[self._format_parameter_set(p) for p in batch]
            )
            try:
                self._client.batch_execute_statement(**batch_execute_statement_args)
            except self._client.exceptions.BadRequestException as e:
                raise self._get_database_error(e) from e

    def scroll(self, value, mode="relative"):
        if not self._paging_state:
            raise InterfaceError("Cursor scroll attempted but pagination is not active")
        scroll_stmt = "MOVE {mode} {value} FROM {pg_cursor_name}".format(
            mode=mode.upper(), value=value, **self._paging_state
        )
        scroll_args = dict(self._paging_state["execute_statement_args"], sql=scroll_stmt)
        logger.debug("Scrolling cursor %s by %d rows", mode, value)
        self._client.execute_statement(**scroll_args)

    def __iter__(self):
        if self._paging_state:
            next_page_args = self._paging_state["execute_statement_args"]
            while True:
                logger.debug(
                    "Fetching page of %d records for auto-paginated query", self._paging_state["records_per_page"]
                )
                next_page_args["sql"] = "FETCH {records_per_page} FROM {pg_cursor_name}".format(**self._paging_state)
                try:
                    page = self._client.execute_statement(**next_page_args)
                except self._client.exceptions.BadRequestException as e:
                    cur_rpp = self._paging_state["records_per_page"]
                    if "Database returned more than the allowed response size limit" in str(e) and cur_rpp > 1:
                        self.scroll(-self._paging_state["records_per_page"])  # Rewind the cursor to read the page again
                        logger.debug("Halving records per page")
                        self._paging_state["records_per_page"] //= 2
                        continue
                    else:
                        raise self._get_database_error(e) from e

                if "columnMetadata" in page and not self.description:
                    self._set_description(page["columnMetadata"])
                if len(page["records"]) == 0:
                    break
                page = self._render_response(page)
                for record in page["records"]:
                    yield record
        else:
            for record in self._current_response.get("records", []):
                yield record

    def fetchone(self):
        try:
            return next(self._iterator)
        except StopIteration:
            pass

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize
        results = []
        while size > 0:
            result = self.fetchone()
            if result is None:
                break
            results.append(result)
            size -= 1
        return results

    def fetchall(self):
        return list(self._iterator)

    def __enter__(self):
        return self

    def __exit__(self, err_type, value, traceback):
        self._iterator = None
        self._current_response = None


def connect(
    aurora_cluster_arn=None,
    secret_arn=None,
    rds_data_client=None,
    database=None,
    host=None,
    port=None,
    username=None,
    password=None,
    charset=None,
    continue_after_timeout=None,
):
    return SyncAuroraDataAPIClient(
        dbname=database,
        aurora_cluster_arn=aurora_cluster_arn,
        secret_arn=secret_arn,
        rds_data_client=rds_data_client,
        charset=charset,
        continue_after_timeout=continue_after_timeout,
    )
