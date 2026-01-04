namespace Leger {
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Extensions for <see cref="IDbCommand"/>.
    /// </summary>
    public static class IDbCommandExtensions {
        /// <summary>
        /// Executes a command.
        /// </summary>
        public static void Execute(
            this IDbCommand command,
            string commandText,
            DbParams? dbParams = null,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30) {
            command.Prepare(commandText, dbParams ?? new DbParams(), commandType, commandTimeout);
            command.TryOpenConnection();

            try {
                command.ExecuteNonQuery();
            }
            catch (Exception ex) {
                throw new DatabaseExecutionException(DatabaseErrorCode.CouldNotExecuteNonQuery, ex);
            }
        }

        /// <summary>
        /// Executes a command asynchronously.
        /// </summary>
        public static async Task ExecuteAsync(
            this IDbCommand command,
            string commandText,
            DbParams? dbParams = null,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null) {
            command.Prepare(commandText, dbParams ?? new DbParams(), commandType, commandTimeout);

            if (command is DbCommand dbCommand) {
                dbCommand.TryOpenConnection();

                try {
                    await dbCommand.ExecuteNonQueryAsync(cancellationToken ?? CancellationToken.None);
                }
                catch (Exception ex) {
                    throw new DatabaseExecutionException(DatabaseErrorCode.CouldNotExecuteNonQuery, ex);
                }
            }
        }

        /// <summary>
        /// Executes a command many times.
        /// </summary>
        public static void ExecuteMany(
            this IDbCommand command,
            string commandText,
            IEnumerable<DbParams> paramList,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30) {
            command.Prepare(commandText, new DbParams(), commandType, commandTimeout);
            command.TryOpenConnection();

            try {
                foreach (var param in paramList) {
                    command.SetDbParams(param);
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex) {
                throw new DatabaseExecutionException(DatabaseErrorCode.CouldNotExecuteNonQuery, ex);
            }
        }

        /// <summary>
        /// Executes a command many times asynchronously.
        /// </summary>
        public static async Task ExecuteManyAsync(
            this IDbCommand command,
            string commandText,
            IEnumerable<DbParams> paramList,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null) {
            command.Prepare(commandText, new DbParams(), commandType, commandTimeout);

            if (command is DbCommand dbCommand) {
                dbCommand.TryOpenConnection();
                var ct = cancellationToken ?? CancellationToken.None;

                try {
                    foreach (var param in paramList) {
                        command.SetDbParams(param);
                        await dbCommand.ExecuteNonQueryAsync(ct);
                    }
                }
                catch (Exception ex) {
                    throw new DatabaseExecutionException(DatabaseErrorCode.CouldNotExecuteNonQuery, ex);
                }
            }
        }

        /// <summary>
        /// Executes a command and returns a scalar.
        /// </summary>
        public static object? Scalar(
            this IDbCommand command,
            string commandText,
            DbParams? dbParams = null,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30) {
            command.Prepare(commandText, dbParams ?? new DbParams(), commandType, commandTimeout);
            command.TryOpenConnection();

            try {
                var result = command.ExecuteScalar();

                if (result == DBNull.Value) {
                    return null;
                }

                return result;
            }
            catch (Exception ex) {
                throw new DatabaseExecutionException(DatabaseErrorCode.CouldNotExecuteScalar, ex);
            }
        }

        /// <summary>
        /// Executes a command and returns a scalar asynchronously.
        /// </summary>
        public static async Task<object?> ScalarAsync(
            this IDbCommand command,
            string commandText,
            DbParams? dbParams = null,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null) {
            command.Prepare(commandText, dbParams ?? new DbParams(), commandType, commandTimeout);

            if (command is DbCommand dbCommand) {
                dbCommand.TryOpenConnection();

                try {
                    var result = await dbCommand.ExecuteScalarAsync(cancellationToken ?? CancellationToken.None);

                    if (result == DBNull.Value) {
                        return null;
                    }

                    return result;
                }
                catch (Exception ex) {
                    throw new DatabaseExecutionException(DatabaseErrorCode.CouldNotExecuteScalar, ex);
                }
            }

            return Task.FromResult<object?>(default);
        }

        /// <summary>
        /// Executes a command and returns an enumerable of type <typeparamref name="T"/>.
        /// </summary>
        public static IEnumerable<T> Query<T>(
            this IDbCommand command,
            string commandText,
            DbParams dbParams,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30) {
            command.Prepare(commandText, dbParams, commandType, commandTimeout);
            command.TryOpenConnection();

            using var rd = command.TryExecuteReader(commandBehavior);

            try {
                return rd.Map(map) ?? Enumerable.Empty<T>();
            }
            catch (Exception ex) {
                if (ex is DatabaseReadFieldException) {
                    throw;
                }

                throw new DatabaseExecutionException(DatabaseErrorCode.CouldNotMapDataReader, ex);
            }
        }

        /// <summary>
        /// Executes a command and returns an enumerable of type <typeparamref name="T"/>.
        /// </summary>
        public static IEnumerable<T> Query<T>(
            this IDbCommand command,
            string commandText,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30) =>
            command.Query(commandText, new DbParams(), map, commandBehavior, commandType, commandTimeout);

        /// <summary>
        /// Executes a command and returns an enumerable of type <typeparamref name="T"/> asynchronously.
        /// </summary>
        public static async Task<IEnumerable<T>> QueryAsync<T>(
            this IDbCommand command,
            string commandText,
            DbParams dbParams,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null) {
            command.Prepare(commandText, dbParams ?? new DbParams(), commandType, commandTimeout);

            if (command is DbCommand dbCommand) {
                dbCommand.TryOpenConnection();

                using var rd = await dbCommand.TryExecuteReaderAsync(commandBehavior, cancellationToken);

                try {
                    return await rd.MapAsync(map);
                }
                catch (Exception ex) {
                    if (ex is DatabaseReadFieldException) {
                        throw;
                    }

                    throw new DatabaseExecutionException(DatabaseErrorCode.CouldNotMapDataReader, ex);
                }
            }

            return Enumerable.Empty<T>();
        }

        /// <summary>
        /// Executes a command and returns an enumerable of type <typeparamref name="T"/> asynchronously.
        /// </summary>
        public static Task<IEnumerable<T>> QueryAsync<T>(
            this IDbCommand command,
            string commandText,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null) =>
            command.QueryAsync(commandText, new DbParams(), map, commandBehavior, commandType, commandTimeout, cancellationToken);

        /// <summary>
        /// Executes a command and returns a single result of type <typeparamref name="T"/>.
        /// </summary>
        public static T QuerySingle<T>(
            this IDbCommand command,
            string commandText,
            DbParams dbParams,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30) {
            command.Prepare(commandText, dbParams, commandType, commandTimeout);
            command.TryOpenConnection();

            using var rd = command.TryExecuteReader(commandBehavior);

            try {
                return rd.MapFirst(map);
            }
            catch (Exception ex) {
                if (ex is DatabaseReadFieldException) {
                    throw;
                }

                throw new DatabaseExecutionException(DatabaseErrorCode.CouldNotMapDataReaderFirst, ex);
            }
        }

        /// <summary>
        /// Executes a command and returns a single result of type <typeparamref name="T"/>.
        /// </summary>
        public static T QuerySingle<T>(
            this IDbCommand command,
            string commandText,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30) =>
            command.QuerySingle(commandText, new DbParams(), map, commandBehavior, commandType, commandTimeout);

        /// <summary>
        /// Executes a command and returns a single result of type <typeparamref name="T"/> asynchronously.
        /// </summary>
        public static async Task<T> QuerySingleAsync<T>(
            this IDbCommand command,
            string commandText,
            DbParams dbParams,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null) {
            command.Prepare(commandText, dbParams, commandType, commandTimeout);

            if (command is DbCommand dbCommand) {
                dbCommand.TryOpenConnection();
                using var rd = await dbCommand.TryExecuteReaderAsync(commandBehavior, cancellationToken);

                try {
                    return await rd.MapFirstAsync(map);
                }
                catch (Exception ex) {
                    if (ex is DatabaseReadFieldException) {
                        throw;
                    }

                    throw new DatabaseExecutionException(DatabaseErrorCode.CouldNotMapDataReaderFirst, ex);
                }
            }

            return default!;
        }

        /// <summary>
        /// Executes a command and returns a single result of type <typeparamref name="T"/> asynchronously.
        /// </summary>
        public static Task<T> QuerySingleAsync<T>(
            this IDbCommand command,
            string commandText,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null) =>
            command.QuerySingleAsync(commandText, new DbParams(), map, commandBehavior, commandType, commandTimeout, cancellationToken);

        /// <summary>
        /// Executes a command, applies the <paramref name="map"/> function to the
        /// <see cref="IDataReader"/>, and returns the result of type <typeparamref name="T"/>.
        /// </summary>
        public static T Read<T>(
            this IDbCommand command,
            string commandText,
            DbParams dbParams,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30) {
            command.Prepare(commandText, dbParams, commandType, commandTimeout);
            command.TryOpenConnection();

            using var rd = command.TryExecuteReader(commandBehavior);

            try {
                return map(rd);
            }
            catch (Exception ex) {
                if (ex is DatabaseReadFieldException) {
                    throw;
                }

                throw new DatabaseExecutionException(DatabaseErrorCode.CouldNotMapDataReader, ex);
            }
        }

        /// <summary>
        /// Executes a command, applies the <paramref name="map"/> function to the
        /// <see cref="IDataReader"/>, and returns the result of type <typeparamref name="T"/>.
        /// </summary>
        public static T Read<T>(
            this IDbCommand command,
            string commandText,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30) =>
            command.Read(commandText, new DbParams(), map, commandBehavior, commandType, commandTimeout);

        /// <summary>
        /// Executes a command, applies the <paramref name="map"/> function to the
        /// <see cref="IDataReader"/>, and returns the result of type
        /// <typeparamref name="T"/> asynchronously.
        /// </summary>
        public static async Task<T> ReadAsync<T>(
            this IDbCommand command,
            string commandText,
            DbParams dbParams,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null) {
            command.Prepare(commandText, dbParams, commandType, commandTimeout);

            if (command is DbCommand dbCommand) {
                dbCommand.TryOpenConnection();

                using var rd = await dbCommand.TryExecuteReaderAsync(commandBehavior, cancellationToken ?? CancellationToken.None);

                try {
                    return map(rd);
                }
                catch (Exception ex) {
                    if (ex is DatabaseReadFieldException) {
                        throw;
                    }

                    throw new DatabaseExecutionException(DatabaseErrorCode.CouldNotMapDataReader, ex);
                }
            }

            return default!;
        }

        /// <summary>
        /// Executes a command, applies the <paramref name="map"/> function to the
        /// <see cref="IDataReader"/>, and returns the result of type
        /// <typeparamref name="T"/> asynchronously.
        /// </summary>
        public static Task<T> ReadAsync<T>(
            this IDbCommand command,
            string commandText,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null) =>
            command.ReadAsync(commandText, new DbParams(), map, commandBehavior, commandType, commandTimeout, cancellationToken);

        /// <summary>
        /// Executes a command, applies the <paramref name="map"/> function to the
        /// <see cref="IDataReader"/>, and returns the result of type
        /// <typeparamref name="T"/> asynchronously.
        /// </summary>
        public static async Task<T> ReadAsync<T>(
            this IDbCommand command,
            string commandText,
            DbParams dbParams,
            Func<IDataReader, Task<T>> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null) {
            command.Prepare(commandText, dbParams, commandType, commandTimeout);

            if (command is DbCommand dbCommand) {
                dbCommand.TryOpenConnection();

                using var rd = await dbCommand.TryExecuteReaderAsync(commandBehavior, cancellationToken ?? CancellationToken.None);

                try {
                    return await map(rd);
                }
                catch (Exception ex) {
                    if (ex is DatabaseReadFieldException) {
                        throw;
                    }

                    throw new DatabaseExecutionException(DatabaseErrorCode.CouldNotMapDataReader, ex);
                }
            }

            return default!;
        }

        /// <summary>
        /// Executes a command, applies the <paramref name="map"/> function to the
        /// <see cref="IDataReader"/>, and returns the result of type
        /// <typeparamref name="T"/> asynchronously.
        /// </summary>
        public static Task<T> ReadAsync<T>(
            this IDbCommand command,
            string commandText,
            Func<IDataReader, Task<T>> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null) =>
            command.ReadAsync(commandText, new DbParams(), map, commandBehavior, commandType, commandTimeout, cancellationToken);


        /// <summary>
        /// Executes a command and returns an async enumerable of type <typeparamref name="T"/> asynchronously.
        /// </summary>
        public static async IAsyncEnumerable<T> StreamAsync<T>(
            this IDbCommand command,
            string commandText,
            DbParams dbParams,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null) {
            command.Prepare(commandText, dbParams, commandType, commandTimeout);

            if (command is DbCommand dbCommand) {
                dbCommand.TryOpenConnection();

                var rd = await dbCommand.TryExecuteReaderAsync(commandBehavior, cancellationToken);

                try {
                    await foreach (var item in rd.MapStreamAsync(map, cancellationToken)) {
                        yield return item;
                    }
                }
                finally {
                    // Ensure the reader is disposed after enumeration
                    await rd.DisposeAsync();
                }
            }
        }

        /// <summary>
        /// Executes a command and returns an async enumerable of type <typeparamref name="T"/> asynchronously.
        /// </summary>
        public static IAsyncEnumerable<T> StreamAsync<T>(
            this IDbCommand command,
            string commandText,
            Func<IDataReader, T> map,
            CommandBehavior commandBehavior = CommandBehavior.Default,
            CommandType commandType = CommandType.Text,
            int commandTimeout = 30,
            CancellationToken? cancellationToken = null) =>
            command.StreamAsync(commandText, new DbParams(), map, commandBehavior, commandType, commandTimeout, cancellationToken);


        internal static void Prepare(
            this IDbCommand command,
            string commandText,
            DbParams dbParams,
            CommandType commandType,
            int commandTimeout) {
            if (commandText is null) {
                throw new DatabaseExecutionException(DatabaseErrorCode.NoCommandText);
            }

            if (!Enum.IsDefined(typeof(CommandType), commandType)) {
                throw new DatabaseExecutionException(DatabaseErrorCode.InvalidCommandType);
            }

            if (commandTimeout < 0) {
                commandTimeout = 30; // reset to default
            }

            command.CommandType = commandType;
            command.CommandText = commandText;
            command.SetDbParams(dbParams);
            command.CommandTimeout = commandTimeout;
        }

        internal static void SetDbParams(this IDbCommand command, DbParams param) {
            command.Parameters.Clear();

            if (param != null) {
                foreach (var p in param) {
                    var commandParam = command.CreateParameter();
                    commandParam.ParameterName = p.Key;

                    if (p.Value is DbTypeParam dbTypeParam) {
                        commandParam.DbType = dbTypeParam.DbType;
                        commandParam.Value = dbTypeParam.Value ?? DBNull.Value;
                    }
                    else {
                        commandParam.Value = p.Value ?? DBNull.Value;
                    }

                    command.Parameters.Add(commandParam);
                }
            }
        }

        private static IDataReader TryExecuteReader(
            this IDbCommand command,
            CommandBehavior commandBehavior) {
            command.TryOpenConnection();

            try {
                return command.ExecuteReader(commandBehavior);
            }
            catch (Exception ex) {
                if (ex is DatabaseReadFieldException) {
                    throw;
                }

                throw new DatabaseExecutionException(DatabaseErrorCode.CouldNotExecuteReader, ex);
            }
        }

        private static async Task<DbDataReader> TryExecuteReaderAsync(
            this DbCommand command,
            CommandBehavior commandBehavior,
            CancellationToken? cancellationToken = null) {
            command.TryOpenConnection();

            try {
                return await command.ExecuteReaderAsync(commandBehavior, cancellationToken ?? CancellationToken.None);
            }
            catch (Exception ex) {
                if (ex is DatabaseReadFieldException) {
                    throw;
                }

                throw new DatabaseExecutionException(DatabaseErrorCode.CouldNotExecuteReader, ex);
            }
        }

        private static void TryOpenConnection(this IDbCommand command) {
            if (command.Connection is IDbConnection connection) {
                connection.TryOpen();
            }
            else {
                throw new DatabaseConnectionException();
            }
        }
    }
}
