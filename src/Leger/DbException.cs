namespace Leger;

using System;

/// <summary>
/// Database error codes.
/// </summary>
public enum DatabaseErrorCode
{
    /// <summary> Could not open connection. </summary>
    CouldNotOpenConnection = 1000,

    /// <summary>Could not begin transaction. </summary>
    CouldNotBeginTransaction = 2000,

    /// <summary>Could not commit transaction. </summary>
    CouldNotExecuteNonQuery = 4000,
    /// <summary>Could not execute scalar. </summary>
    CouldNotExecuteScalar = 4001,
    /// <summary>Could not execute reader. </summary>
    CouldNotExecuteReader = 4002,

    /// <summary>Could not cast value. </summary>
    CouldNotCastValue = 5000
}

/// <summary>
/// Represents a database exception.
/// </summary>
/// <param name="errorCode"></param>
/// <param name="message"></param>
/// <param name="innerEx"></param>
public class DatabaseException(DatabaseErrorCode errorCode, string message, Exception? innerEx = null) : Exception(message, innerEx)
{
    /// <summary> Gets the error code. </summary>
    public readonly DatabaseErrorCode ErrorCode = errorCode;
}

/// <summary>
/// Represents a database connection exception.
/// </summary>
public class DatabaseConnectionException()
    : DatabaseException(
        DatabaseErrorCode.CouldNotOpenConnection,
        $"The connection is not currently available to open.")
{
}

/// <summary>
/// Represents a database transaction exception.
/// </summary>
public class DatabaseTransactionException()
    : DatabaseException(
        DatabaseErrorCode.CouldNotBeginTransaction,
        $"Could not begin transaction.")
{
}

/// <summary>
/// Represents a database execution exception.
/// </summary>
/// <param name="errorCode"></param>
/// <param name="sql"></param>
/// <param name="ex"></param>
public class DatabaseExecutionException(
    DatabaseErrorCode errorCode,
    string sql,
    Exception? ex = null)
    : DatabaseException(errorCode, sql, ex)
{
}
