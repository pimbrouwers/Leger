namespace Leger;

using System;

public enum DatabaseErrorCode
{
    CouldNotOpenConnection = 1000,

    CouldNotBeginTransaction = 2000,

    CouldNotExecuteNonQuery = 4000,
    CouldNotExecuteScalar = 4001,
    CouldNotExecuteReader = 4002,

    CouldNotCastValue = 5000
}

public class DatabaseException(DatabaseErrorCode errorCode, string message, Exception? innerEx = null) : Exception(message, innerEx)
{
    public readonly DatabaseErrorCode ErrorCode = errorCode;
}

public class DatabaseConnectionException()
    : DatabaseException(
        DatabaseErrorCode.CouldNotOpenConnection,
        $"The connection is not currently available to open.")
{
}

public class DatabaseTransactionException()
    : DatabaseException(
        DatabaseErrorCode.CouldNotBeginTransaction,
        $"Could not begin transaction.")
{
}

public class DatabaseExecutionException(
    DatabaseErrorCode errorCode,
    string sql,
    Exception? ex = null)
    : DatabaseException(errorCode, sql, ex)
{
}
