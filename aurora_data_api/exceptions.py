from .mysql_error_codes import MySQLErrorCodes
from .postgresql_error_codes import PostgreSQLErrorCodes


class Warning(Exception):
    pass


class Error(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


class _DatabaseErrorFactory:
    def __getattr__(self, a):
        err_cls = type(getattr(self.err_index, a).name, (DatabaseError,), {})
        setattr(self, a, err_cls)
        return err_cls

    def from_code(self, err_code):
        return getattr(self, self.err_index(err_code).name)


class _MySQLErrorFactory(_DatabaseErrorFactory):
    err_index = MySQLErrorCodes


class _PostgreSQLErrorFactory(_DatabaseErrorFactory):
    err_index = PostgreSQLErrorCodes


MySQLError = _MySQLErrorFactory()
PostgreSQLError = _PostgreSQLErrorFactory()
