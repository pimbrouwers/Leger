namespace Leger
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Extensions for <see cref="IDbConnection"/>.
    /// </summary>
    public static class IDbConnectionExtensions
    {
        /// <summary>
        /// Executes a command.
        /// </summary>
        public static void Execute(
            this IDbConnection connection,
            string commandText,
            DbParams? dbParams = null,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30)
        {
            using (var command = connection.CreateCommand())
            {
                command.Execute(commandText, dbParams, commandType, commandTimeout);
            }
        }

        /// <summary>
        /// Executes a command asynchronously.
        /// </summary>
        public static async Task ExecuteAsync(
            this IDbConnection connection,
            string commandText,
            DbParams? dbParams = null,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null)
        {
            using (var command = connection.CreateCommand())
            {
                await command.ExecuteAsync(commandText, dbParams ?? new DbParams(), commandType, commandTimeout, cancellationToken);
            }
        }

        /// <summary>
        /// Executes a command many times.
        /// </summary>
        public static void ExecuteMany(
            this IDbConnection connection,
            string commandText,
            IEnumerable<DbParams> paramList,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30)
        {
            using (var command = connection.CreateCommand())
            {
                command.ExecuteMany(commandText, paramList, commandType, commandTimeout);
            }
        }

        /// <summary>
        /// Executes a command many times asynchronously.
        /// </summary>
        public static async Task ExecuteManyAsync(
            this IDbConnection connection,
            string commandText,
            IEnumerable<DbParams> paramList,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null)
        {
            using (var command = connection.CreateCommand())
            {
                await command.ExecuteManyAsync(commandText, paramList, commandType, commandTimeout, cancellationToken);
            }
        }

        /// <summary>
        /// Executes a command and returns a scalar.
        /// </summary>
        public static object? Scalar(
            this IDbConnection connection,
            string commandText,
            DbParams? dbParams = null,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30)
        {
            using (var command = connection.CreateCommand())
            {
                return command.Scalar(commandText, dbParams ?? new DbParams(), commandType, commandTimeout);
            }
        }

        /// <summary>
        /// Executes a command and returns a scalar asynchronously.
        /// </summary>
        public static async Task<object?> ScalarAsync(
            this IDbConnection connection,
            string commandText,
            DbParams? dbParams = null,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null)
        {
            using (var command = connection.CreateCommand())
            {
                return await command.ScalarAsync(commandText, dbParams ?? new DbParams(), commandType, commandTimeout, cancellationToken);
            }
        }

        /// <summary>
        /// Executes a command and returns an enumerable of type <typeparamref name="T"/>.
        /// </summary>
        public static IEnumerable<T> Query<T>(
            this IDbConnection connection,
            string commandText,
            DbParams dbParams,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30)
        {
            using (var command = connection.CreateCommand())
            {
                return command.Query(commandText, dbParams, map, commandBehavior, commandType, commandTimeout);
            }
        }

        /// <summary>
        /// Executes a command and returns an enumerable of type <typeparamref name="T"/>.
        /// </summary>
        public static IEnumerable<T> Query<T>(
            this IDbConnection connection,
            string commandText,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30) =>
            connection.Query(commandText, new DbParams(), map, commandBehavior, commandType, commandTimeout);

        /// <summary>
        /// Executes a command and returns an enumerable of type <typeparamref name="T"/> asynchronously.
        /// </summary>
        public static async Task<IEnumerable<T>> QueryAsync<T>(
            this IDbConnection connection,
            string commandText,
            DbParams dbParams,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null)
        {
            using (var command = connection.CreateCommand())
            {
                return await command.QueryAsync(commandText, dbParams, map, commandBehavior, commandType, commandTimeout, cancellationToken);
            }
        }

        /// <summary>
        /// Executes a command and returns an enumerable of type <typeparamref name="T"/> asynchronously.
        /// </summary>
        public static async Task<IEnumerable<T>> QueryAsync<T>(
            this IDbConnection connection,
            string commandText,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null) =>
            await connection.QueryAsync(commandText, new DbParams(), map, commandBehavior, commandType, commandTimeout, cancellationToken);

        /// <summary>
        /// Executes a command and returns a single result of type <typeparamref name="T"/>.
        /// </summary>
        public static T QuerySingle<T>(
            this IDbConnection connection,
            string commandText,
            DbParams dbParams,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30)
        {
            using (var command = connection.CreateCommand())
            {
                return command.QuerySingle(commandText, dbParams, map, commandBehavior, commandType, commandTimeout);
            }
        }

        /// <summary>
        /// Executes a command and returns a single result of type <typeparamref name="T"/>.
        /// </summary>
        public static T QuerySingle<T>(
            this IDbConnection connection,
            string commandText,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30) =>
            connection.QuerySingle(commandText, new DbParams(), map, commandBehavior, commandType, commandTimeout);

        /// <summary>
        /// Executes a command and returns a single result of type <typeparamref name="T"/> asynchronously.
        /// </summary>
        public static async Task<T> QuerySingleAsync<T>(
            this IDbConnection connection,
            string commandText,
            DbParams dbParams,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null)
        {
            using (var command = connection.CreateCommand())
            {
                return await command.QuerySingleAsync(commandText, dbParams, map, commandBehavior, commandType, commandTimeout, cancellationToken);
            }
        }

        /// <summary>
        /// Executes a command and returns a single result of type <typeparamref name="T"/> asynchronously.
        /// </summary>
        public static async Task<T> QuerySingleAsync<T>(
            this IDbConnection connection,
            string commandText,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null) =>
            await connection.QuerySingleAsync(commandText, new DbParams(), map, commandBehavior, commandType, commandTimeout, cancellationToken);

        /// <summary>
        /// Executes a command, applies the <paramref name="map"/> function to the
        /// <see cref="IDataReader"/>, and returns the result of type <typeparamref name="T"/>.
        /// </summary>
        public static T Read<T>(
            this IDbConnection connection,
            string commandText,
            DbParams dbParams,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30)
        {
            using (var command = connection.CreateCommand())
            {
                return command.Read(commandText, dbParams, map, commandBehavior, commandType, commandTimeout);
            }
        }

        /// <summary>
        /// Executes a command, applies the <paramref name="map"/> function to the
        /// <see cref="IDataReader"/>, and returns the result of type
        /// <typeparamref name="T"/>.
        /// </summary>
        public static T Read<T>(
            this IDbConnection connection,
            string commandText,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30) =>
            connection.Read(commandText, new DbParams(), map, commandBehavior, commandType, commandTimeout);

        /// <summary>
        /// Executes a command, applies the <paramref name="map"/> function to the
        /// <see cref="IDataReader"/>, and returns the result of type
        /// <typeparamref name="T"/> asynchronously.
        /// </summary>
        public static async Task<T> ReadAsync<T>(
            this IDbConnection connection,
            string commandText,
            DbParams dbParams,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null)
        {
            using (var command = connection.CreateCommand())
            {
                return await command.ReadAsync(commandText, dbParams, map, commandBehavior, commandType, commandTimeout, cancellationToken);
            }
        }

        /// <summary>
        /// Executes a command, applies the <paramref name="map"/> function to the
        /// <see cref="IDataReader"/>, and returns the result of type
        /// <typeparamref name="T"/> asynchronously.
        /// </summary>
        public static Task<T> ReadAsync<T>(
            this IDbConnection connection,
            string commandText,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null) =>
            connection.ReadAsync(commandText, new DbParams(), map, commandBehavior, commandType, commandTimeout, cancellationToken);

        /// <summary>
        /// Attempts to safely open the <see cref="IDbConnection"/> and begin a
        /// transaction.
        /// </summary>
        public static IDbTransaction CreateTransaction(this IDbConnection connection)
        {
            connection.TryOpen();
            return connection.BeginTransaction();
        }

        internal static void TryOpen(this IDbConnection connection)
        {
            if (connection.State == ConnectionState.Closed)
            {
                connection.Open();
            }

            if (connection.State != ConnectionState.Open)
            {
                throw new DatabaseConnectionException();
            }
        }

    }
}
