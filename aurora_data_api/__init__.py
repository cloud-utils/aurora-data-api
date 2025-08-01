# For backward compatibility
from .sync import connect
from .sync import SyncAuroraDataAPIClient as AuroraDataAPIClient
from .sync import SyncAuroraDataAPICursor as AuroraDataAPICursor

# Import all DB-API constants from base
from .base import (
    apilevel,
    threadsafety,
    paramstyle,
    Date,
    Time,
    Timestamp,
    DateFromTicks,
    # TimeFromTicks,
    TimestampFromTicks,
    Binary,
    STRING,
    BINARY,
    NUMBER,
    DATETIME,
    ROWID,
    DECIMAL,
)
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