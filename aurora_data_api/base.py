"""
Base classes for Aurora Data API clients and cursors
"""
import os, datetime, ipaddress, uuid, time, random, string, logging, itertools, reprlib, json, re
from decimal import Decimal
from collections import namedtuple
from collections.abc import Mapping
from .exceptions import (
    Warning,
    Error,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError,
    MySQLError,
    PostgreSQLError,
)
from .error_codes_mysql import MySQLErrorCodes
from .error_codes_postgresql import PostgreSQLErrorCodes

apilevel = "2.0"

threadsafety = 0

paramstyle = "named"

Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
DateFromTicks = datetime.date.fromtimestamp
# TimeFromTicks = datetime.time.fromtimestamp TODO
TimestampFromTicks = datetime.datetime.fromtimestamp
Binary = bytes
STRING = str
BINARY = bytes
NUMBER = float
DATETIME = datetime.datetime
ROWID = str
DECIMAL = Decimal

ColumnDescription = namedtuple("ColumnDescription", "name type_code display_size internal_size precision scale null_ok")
ColumnDescription.__new__.__defaults__ = (None,) * len(ColumnDescription._fields)

logger = logging.getLogger(__name__)


class BaseAuroraDataAPIClient:
    """Base class for Aurora Data API clients"""
    
    def __init__(
        self,
        dbname=None,
        aurora_cluster_arn=None,
        secret_arn=None,
        rds_data_client=None,
        charset=None,
        continue_after_timeout=None,
    ):
        self._client = rds_data_client
        self._dbname = dbname
        self._aurora_cluster_arn = aurora_cluster_arn or os.environ.get("AURORA_CLUSTER_ARN")
        self._secret_arn = secret_arn or os.environ.get("AURORA_SECRET_ARN")
        self._charset = charset
        self._transaction_id = None
        self._continue_after_timeout = continue_after_timeout

    def close(self):
        pass


class BaseAuroraDataAPICursor:
    """Base class for Aurora Data API cursors"""
    
    _pg_type_map = {
        "int": int,
        "int2": int,
        "int4": int,
        "int8": int,
        "float4": float,
        "float8": float,
        "serial2": int,
        "serial4": int,
        "serial8": int,
        "bool": bool,
        "varbit": bytes,
        "bytea": bytearray,
        "char": str,
        "varchar": str,
        "cidr": ipaddress.ip_network,
        "date": datetime.date,
        "inet": ipaddress.ip_address,
        "json": dict,
        "jsonb": dict,
        "money": str,
        "text": str,
        "time": datetime.time,
        "timestamp": datetime.datetime,
        "uuid": uuid.uuid4,
        "numeric": Decimal,
        "decimal": Decimal,
    }
    _data_api_type_map = {
        bytes: "blobValue",
        bool: "booleanValue",
        float: "doubleValue",
        int: "longValue",
        str: "stringValue",
        Decimal: "stringValue",
        # list: "arrayValue"
    }
    _data_api_type_hint_map = {
        datetime.date: "DATE",
        datetime.time: "TIME",
        datetime.datetime: "TIMESTAMP",
        Decimal: "DECIMAL",
    }

    def __init__(
        self,
        client=None,
        dbname=None,
        aurora_cluster_arn=None,
        secret_arn=None,
        transaction_id=None,
        continue_after_timeout=None,
    ):
        self.arraysize = 1000
        self.description = None
        self._client = client
        self._dbname = dbname
        self._aurora_cluster_arn = aurora_cluster_arn
        self._secret_arn = secret_arn
        self._transaction_id = transaction_id
        self._current_response = None
        self._iterator = None
        self._paging_state = None
        self._continue_after_timeout = continue_after_timeout

    def prepare_param(self, param_name, param_value):
        if param_value is None:
            return dict(name=param_name, value=dict(isNull=True))
        param_data_api_type = self._data_api_type_map.get(type(param_value), "stringValue")
        param = dict(name=param_name, value={param_data_api_type: param_value})
        if param_data_api_type == "stringValue" and not isinstance(param_value, str):
            param["value"][param_data_api_type] = str(param_value)
        if type(param_value) in self._data_api_type_hint_map:
            param["typeHint"] = self._data_api_type_hint_map[type(param_value)]
        return param

        # if param_data_api_type == "arrayValue" and len(param_value) > 0:
        #     return {
        #         param_data_api_type: {
        #             self._data_api_type_map.get(type(param_value[0]), "stringValue") + "s": param_value
        #         }
        #     }

    def _set_description(self, column_metadata):
        # see https://www.postgresql.org/docs/9.5/datatype.html
        self.description = []
        for column in column_metadata:
            col_desc = ColumnDescription(
                name=column["name"], type_code=self._pg_type_map.get(column["typeName"].lower(), str)
            )
            self.description.append(col_desc)

    def _prepare_execute_args(self, operation):
        execute_args = dict(
            database=self._dbname, resourceArn=self._aurora_cluster_arn, secretArn=self._secret_arn, sql=operation
        )
        if self._transaction_id:
            execute_args["transactionId"] = self._transaction_id
        return execute_args

    def _format_parameter_set(self, parameters):
        if not isinstance(parameters, Mapping):
            raise NotSupportedError("Expected a mapping of parameters. Array parameters are not supported.")
        return [self.prepare_param(k, v) for k, v in parameters.items()]

    def _get_database_error(self, original_error):
        error_msg = getattr(original_error, "response", {}).get("Error", {}).get("Message", "")
        try:
            res = re.search(r"Error code: (\d+); SQLState: (\d+)$", error_msg)
            if res:  # MySQL error
                error_code = int(res.group(1))
                error_class = MySQLError.from_code(error_code)
                error = error_class(error_msg)
                error.response = getattr(original_error, "response", {})
                return error
            res = re.search(r"ERROR: .*(?:\n |;) Position: (\d+); SQLState: (\w+)$", error_msg)
            if res:  # PostgreSQL error
                error_code = res.group(2)
                error_class = PostgreSQLError.from_code(error_code)
                error = error_class(error_msg)
                error.response = getattr(original_error, "response", {})
                return error
        except Exception:
            pass
        return DatabaseError(original_error)

    @property
    def rowcount(self):
        if self._current_response:
            if "records" in self._current_response:
                return len(self._current_response["records"])
            elif "numberOfRecordsUpdated" in self._current_response:
                return self._current_response["numberOfRecordsUpdated"]
        return -1

    @property
    def lastrowid(self):
        # TODO: this may not make sense if the previous statement is not an INSERT
        if self._current_response and self._current_response.get("generatedFields"):
            return self._render_value(self._current_response["generatedFields"][-1])

    def _page_input(self, iterable, page_size=1000):
        iterable = iter(iterable)
        return iter(lambda: list(itertools.islice(iterable, page_size)), [])

    def _render_response(self, response):
        if "records" in response:
            for i, record in enumerate(response["records"]):
                response["records"][i] = tuple(
                    self._render_value(value, col_desc=self.description[j] if self.description else None)
                    for j, value in enumerate(record)
                )
        return response

    def _render_value(self, value, col_desc=None):
        if value.get("isNull"):
            return None
        elif "arrayValue" in value:
            if "arrayValues" in value["arrayValue"]:
                return [self._render_value(nested) for nested in value["arrayValue"]["arrayValues"]]
            else:
                return list(value["arrayValue"].values())[0]
        else:
            scalar_value = list(value.values())[0]
            if col_desc and col_desc.type_code in self._data_api_type_hint_map:
                if col_desc.type_code == Decimal:
                    scalar_value = Decimal(scalar_value)
                else:
                    try:
                        scalar_value = col_desc.type_code.fromisoformat(scalar_value)
                    except (AttributeError, ValueError):  # fromisoformat not supported on Python < 3.7
                        if col_desc.type_code == datetime.date:
                            scalar_value = datetime.datetime.strptime(scalar_value, "%Y-%m-%d").date()
                        elif col_desc.type_code == datetime.time:
                            scalar_value = datetime.datetime.strptime(scalar_value, "%H:%M:%S").time()
                        elif "." in scalar_value:
                            scalar_value = datetime.datetime.strptime(scalar_value, "%Y-%m-%d %H:%M:%S.%f")
                        else:
                            scalar_value = datetime.datetime.strptime(scalar_value, "%Y-%m-%d %H:%M:%S")
            return scalar_value

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    def close(self):
        pass
