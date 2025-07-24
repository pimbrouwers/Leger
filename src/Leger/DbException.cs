namespace Leger
{
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
        CouldNotCastValue = 5000,

        /// <summary>Field not found in the database record.</summary>
        FieldNotFound = 6000,
    }

    /// <summary>
    /// Represents a database exception.
    /// </summary>
    public class DatabaseException : Exception
    {
        /// <summary> Gets the error code. </summary>
        public readonly DatabaseErrorCode ErrorCode;

        /// <summary>
        /// Initializes a new instance of the <see cref="DatabaseException"/> class.
        /// This exception is thrown when a database operation fails.
        /// </summary>
        /// <param name="errorCode"></param>
        /// <param name="message"></param>
        /// <param name="innerEx"></param>
        public DatabaseException(
            DatabaseErrorCode errorCode,
            string message,
            Exception? innerEx = null) :
            base(message, innerEx)
        {
            ErrorCode = errorCode;
        }
    }

    /// <summary>
    /// Represents a database connection exception.
    /// </summary>
    public class DatabaseConnectionException : DatabaseException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DatabaseConnectionException"/> class.
        /// This exception is thrown when the database connection is not available to open.
        /// </summary>
        public DatabaseConnectionException() :
            base(DatabaseErrorCode.CouldNotOpenConnection, $"The connection is not currently available to open.")
        {
        }
    }

    /// <summary>
    /// Represents a database transaction exception.
    /// </summary>
    public class DatabaseTransactionException : DatabaseException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DatabaseTransactionException"/> class.
        /// This exception is thrown when a transaction could not be started.
        /// </summary>
        public DatabaseTransactionException() :
            base(DatabaseErrorCode.CouldNotBeginTransaction, $"Could not begin transaction.")
        {
        }
    }

    /// <summary>
    /// Represents a database execution exception.
    /// </summary>
    public class DatabaseExecutionException : DatabaseException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DatabaseExecutionException"/> class.
        /// This exception is thrown when a database operation fails to execute.
        /// </summary>
        /// <param name="errorCode"></param>
        /// <param name="sql"></param>
        /// <param name="ex"></param>
        public DatabaseExecutionException(
            DatabaseErrorCode errorCode,
            string sql,
            Exception? ex = null) : base(errorCode, sql, ex)
        { }
    }
}
