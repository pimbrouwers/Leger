namespace Leger
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// IDbTransaction extensions.
    /// </summary>
    public static class IDbTransactionExtensions
    {
        /// <summary>
        /// Executes a command.
        /// </summary>
        public static void Execute(
            this IDbTransaction transaction,
            string commandText,
            DbParams? dbParams = null,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30)
        {
            using (var command = transaction.CreateTransactionCommand())
            {
                command.Execute(commandText, dbParams, commandType, commandTimeout);
            }
        }

        /// <summary>
        /// Executes a command asynchronously.
        /// </summary>
        public static async Task ExecuteAsync(
            this IDbTransaction transaction,
            string commandText,
            DbParams? dbParams = null,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null)
        {
            using (var command = transaction.CreateTransactionCommand())
            {
                await command.ExecuteAsync(commandText, dbParams, commandType, commandTimeout, cancellationToken);
            }
        }

        /// <summary>
        /// Executes a command many times.
        /// </summary>
        public static void ExecuteMany(
            this IDbTransaction transaction,
            string commandText,
            IEnumerable<DbParams> paramList,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30)
        {
            using (var command = transaction.CreateTransactionCommand())
            {
                command.ExecuteMany(commandText, paramList, commandType, commandTimeout);
            }
        }

        /// <summary>
        /// Executes a command many times asynchronously.
        /// </summary>
        public static async Task ExecuteManyAsync(
            this IDbTransaction transaction,
            string commandText,
            IEnumerable<DbParams> paramList,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null)
        {
            using (var command = transaction.CreateTransactionCommand())
            {
                await command.ExecuteManyAsync(commandText, paramList, commandType, commandTimeout, cancellationToken);
            }
        }

        /// <summary>
        /// Executes a command and returns a scalar.
        /// </summary>
        public static object? Scalar(
            this IDbTransaction transaction,
            string commandText,
            DbParams? dbParams = null,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30)
        {
            using (var command = transaction.CreateTransactionCommand())
            {
                return command.Scalar(commandText, dbParams ?? new DbParams(), commandType, commandTimeout);
            }
        }

        /// <summary>
        /// Executes a command and returns a scalar asynchronously.
        /// </summary>
        public static async Task<object?> ScalarAsync(
            this IDbTransaction transaction,
            string commandText,
            DbParams? dbParams = null,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null)
        {
            using (var command = transaction.CreateTransactionCommand())
            {
                return await command.ScalarAsync(commandText, dbParams ?? new DbParams(), commandType, commandTimeout, cancellationToken);
            }
        }

        /// <summary>
        /// Executes a command and returns an enumerable of type <typeparamref name="T"/>.
        /// </summary>
        public static IEnumerable<T> Query<T>(
            this IDbTransaction transaction,
            string commandText,
            DbParams dbParams,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30)
        {
            using (var command = transaction.CreateTransactionCommand())
            {
                return command.Query(commandText, dbParams, map, commandBehavior, commandType, commandTimeout);
            }
        }

        /// <summary>
        /// Executes a command and returns an enumerable of type <typeparamref name="T"/>.
        /// </summary>
        public static IEnumerable<T> Query<T>(
            this IDbTransaction transaction,
            string commandText,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30) =>
            transaction.Query(commandText, new DbParams(), map, commandBehavior, commandType, commandTimeout);

        /// <summary>
        /// Executes a command and returns an enumerable of type <typeparamref name="T"/> asynchronously.
        /// </summary>
        public static async Task<IEnumerable<T>> QueryAsync<T>(
            this IDbTransaction transaction,
            string commandText,
            DbParams dbParams,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null)
        {
            using (var command = transaction.CreateTransactionCommand())
            {
                return await command.QueryAsync(commandText, dbParams, map, commandBehavior, commandType, commandTimeout, cancellationToken);
            }
        }

        /// <summary>
        /// Executes a command and returns an enumerable of type <typeparamref name="T"/> asynchronously.
        /// </summary>
        public static async Task<IEnumerable<T>> QueryAsync<T>(
            this IDbTransaction transaction,
            string commandText,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null) =>
            await transaction.QueryAsync(commandText, new DbParams(), map, commandBehavior, commandType, commandTimeout, cancellationToken);

        /// <summary>
        /// Executes a command and returns a single result of type <typeparamref name="T"/>.
        /// </summary>
        public static T QuerySingle<T>(
            this IDbTransaction transaction,
            string commandText,
            DbParams dbParams,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30)
        {
            using (var command = transaction.CreateTransactionCommand())
            {
                return command.QuerySingle(commandText, dbParams, map, commandBehavior, commandType, commandTimeout);
            }
        }

        /// <summary>
        /// Executes a command and returns a single result of type <typeparamref name="T"/>.
        /// </summary>
        public static T QuerySingle<T>(
            this IDbTransaction transaction,
            string commandText,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30) =>
            transaction.QuerySingle(commandText, new DbParams(), map, commandBehavior, commandType, commandTimeout);

        /// <summary>
        /// Executes a command and returns a single result of type <typeparamref name="T"/> asynchronously.
        /// </summary>
        public static async Task<T> QuerySingleAsync<T>(
            this IDbTransaction transaction,
            string commandText,
            DbParams dbParams,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null)
        {
            using (var command = transaction.CreateTransactionCommand())
            {
                return await command.QuerySingleAsync(commandText, dbParams, map, commandBehavior, commandType, commandTimeout, cancellationToken);
            }
        }

        /// <summary>
        /// Executes a command and returns a single result of type <typeparamref name="T"/> asynchronously.
        /// </summary>
        public static async Task<T> QuerySingleAsync<T>(
            this IDbTransaction transaction,
            string commandText,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null) =>
            await transaction.QuerySingleAsync(commandText, new DbParams(), map, commandBehavior, commandType, commandTimeout, cancellationToken);

        /// <summary>
        /// Executes a command, applies the <paramref name="map"/> function to the
        /// <see cref="IDataReader"/>, and returns the result of type <typeparamref name="T"/>.
        /// </summary>
        public static T Read<T>(
            this IDbTransaction transaction,
            string commandText,
            DbParams dbParams,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30)
        {
            using (var command = transaction.CreateTransactionCommand())
            {
                return command.Read(commandText, dbParams, map, commandBehavior, commandType, commandTimeout);
            }
        }

        /// <summary>
        /// Executes a command, applies the <paramref name="map"/> function to the
        /// <see cref="IDataReader"/>, and returns the result of type <typeparamref name="T"/>.
        /// </summary>
        public static T Read<T>(
            this IDbTransaction transaction,
            string commandText,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30) =>
            transaction.Read(commandText, new DbParams(), map, commandBehavior, commandType, commandTimeout);

        /// <summary>
        /// Executes a command, applies the <paramref name="map"/> function to the
        /// <see cref="IDataReader"/>, and returns the result of type
        /// <typeparamref name="T"/> asynchronously.
        /// </summary>
        public static async Task<T> ReadAsync<T>(
            this IDbTransaction transaction,
            string commandText,
            DbParams dbParams,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null)
        {
            using (var command = transaction.CreateTransactionCommand())
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
            this IDbTransaction transaction,
            string commandText,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null) =>
            transaction.ReadAsync(commandText, new DbParams(), map, commandBehavior, commandType, commandTimeout, cancellationToken);

        private static IDbCommand CreateTransactionCommand(
            this IDbTransaction transaction)
        {
            var command =
                transaction.Connection?.CreateCommand() ??
                throw new DatabaseTransactionException();

            command.Transaction = transaction;
            return command;
        }
    }
}
