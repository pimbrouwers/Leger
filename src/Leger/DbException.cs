namespace Leger {
    using System;

    /// <summary>
    /// Database error codes.
    /// </summary>
    public enum DatabaseErrorCode {
        /// <summary> Could not open connection.</summary>
        CouldNotOpenConnection = 1000,

        /// <summary>Could not begin transaction.</summary>
        CouldNotBeginTransaction = 2000,

        /// <summary>Could not commit transaction.</summary>
        CouldNotExecuteNonQuery = 4000,
        /// <summary>Could not execute scalar.</summary>
        CouldNotExecuteScalar = 4001,
        /// <summary>Could not execute reader.</summary>
        CouldNotExecuteReader = 4002,
        /// <summary>Could not execute command because the command text is null or empty.</summary>
        NoCommandText = 4003,
        /// <summary>Could not execute command because the command type is invalid.</summary>
        InvalidCommandType = 4004,

        /// <summary>Could not cast value.</summary>
        CouldNotCastValue = 5000,

        /// <summary>Field not found in the database record.</summary>
        FieldNotFound = 6000,

        /// <summary>Could not map data reader using the provided mapping function.</summary>
        CouldNotMapDataReader = 7000,

        /// <summary>Could not map first record from data reader using the provided mapping function.</summary>
        CouldNotMapDataReaderFirst = 7001
    }

    /// <summary>
    /// Represents a database exception.
    /// </summary>
    public class DatabaseException : Exception {
        /// <summary>
        /// Initializes a new instance of the <see cref="DatabaseException"/> class.
        /// </summary>
        /// <param name="errorCode"></param>
        /// <param name="message"></param>
        /// <param name="innerEx"></param>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public DatabaseException(DatabaseErrorCode errorCode, string message, Exception? innerEx = null)
            : base(message, innerEx) {
            if (!Enum.IsDefined(typeof(DatabaseErrorCode), errorCode)) {
                throw new ArgumentOutOfRangeException(nameof(errorCode), "Invalid database error code.");
            }
            ErrorCode = errorCode;
        }

        /// <summary>
        /// Gets the database error code.
        /// </summary>
        public DatabaseErrorCode ErrorCode { get; }

        /// <summary>
        /// Gets the timestamp when the exception was created.
        /// </summary>
        public DateTime Timestamp { get; } = DateTime.UtcNow;

    }

    /// <summary>
    /// Represents a database connection exception.
    /// </summary>
    public class DatabaseConnectionException : DatabaseException {
        /// <summary>
        /// Initializes a new instance of the <see cref="DatabaseConnectionException"/> class.
        /// This exception is thrown when the database connection is not available to open.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerEx"></param>
        public DatabaseConnectionException(string? message = null, Exception? innerEx = null) :
            base(DatabaseErrorCode.CouldNotOpenConnection, message ?? "The connection is not currently available to open.", innerEx) {
        }
    }

    /// <summary>
    /// Represents a database transaction exception.
    /// </summary>
    public class DatabaseTransactionException : DatabaseException {
        /// <summary>
        /// Initializes a new instance of the <see cref="DatabaseTransactionException"/> class.
        /// This exception is thrown when a transaction could not be started.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerEx"></param>
        public DatabaseTransactionException(string? message = null, Exception? innerEx = null) :
            base(DatabaseErrorCode.CouldNotBeginTransaction, message ?? "Could not begin transaction.", innerEx) {
        }
    }

    /// <summary>
    /// Represents a database execution exception.
    /// </summary>
    public class DatabaseExecutionException : DatabaseException {
        /// <summary>
        /// Initializes a new instance of the <see cref="DatabaseExecutionException"/> class.
        /// </summary>
        /// <param name="errorCode"></param>
        /// <param name="innerEx"></param>
        public DatabaseExecutionException(DatabaseErrorCode errorCode, Exception? innerEx = null)
            : base(errorCode, "A database operation failed. See inner exception for details.", innerEx) {
        }
    }

    /// <summary>
    /// Represents a database execution exception.
    /// </summary>
    public class DatabaseReadFieldException : DatabaseException {
        /// <summary>
        /// Initializes a new instance of the <see cref="DatabaseReadFieldException"/> class.
        /// </summary>
        /// <param name="errorCode"></param>
        /// <param name="fieldName"></param>
        /// <param name="innerEx"></param>
        public DatabaseReadFieldException(DatabaseErrorCode errorCode, string fieldName, Exception? innerEx = null)
            : base(errorCode, $"A database read field operation failed for field '{fieldName}'. See inner exception for details.", innerEx) {
        }
    }
}
