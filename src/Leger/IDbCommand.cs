namespace Leger;

using System.Data;
using System.Data.Common;

/// <summary>
/// Extensions for <see cref="IDbCommand"/>.
/// </summary>
public static class IDbCommandExtensions
{
    /// <summary>
    /// Executes a command.
    /// </summary>
    public static void Execute(
        this IDbCommand command,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text)
    {
        command.Prepare(commandText, dbParams ?? [], commandType);

        command.Do(
            DatabaseErrorCode.CouldNotExecuteNonQuery,
            cmd => cmd.ExecuteNonQuery());
    }

    /// <summary>
    /// Executes a command asynchronously.
    /// </summary>
    public static Task ExecuteAsync(
        this IDbCommand command,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        command.Prepare(commandText, dbParams ?? [], commandType);

        return command.DoAsync(
            DatabaseErrorCode.CouldNotExecuteNonQuery,
            cmd => cmd.ExecuteNonQueryAsync(cancellationToken ?? CancellationToken.None));
    }

    /// <summary>
    /// Executes a command many times.
    /// </summary>
    public static void ExecuteMany(
        this IDbCommand command,
        string commandText,
        IEnumerable<DbParams> paramList,
        CommandType commandType = CommandType.Text)
    {
        command.Prepare(commandText, [], commandType);
        command.DoMany(paramList, command => command.ExecuteNonQuery());
    }

    /// <summary>
    /// Executes a command many times asynchronously.
    /// </summary>
    public static async Task ExecuteManyAsync(
        this IDbCommand command,
        string commandText,
        IEnumerable<DbParams> paramList,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        command.Prepare(commandText, [], commandType);
        await command.DoManyAsync(paramList, command =>
            command.ExecuteNonQueryAsync(cancellationToken ?? CancellationToken.None));
    }

    /// <summary>
    /// Executes a command and returns a scalar.
    /// </summary>
    public static object? Scalar(
        this IDbCommand command,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text)
    {
        command.Prepare(commandText, dbParams ?? [], commandType);

        return command.Do(
            DatabaseErrorCode.CouldNotExecuteScalar,
            cmd => cmd.ExecuteScalar());
    }

    /// <summary>
    /// Executes a command and returns a scalar asynchronously.
    /// </summary>
    public static Task<object?> ScalarAsync(
        this IDbCommand command,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        command.Prepare(commandText, dbParams ?? [], commandType);
        return command.DoAsync(
            DatabaseErrorCode.CouldNotExecuteScalar,
            cmd => cmd.ExecuteScalarAsync(cancellationToken ?? CancellationToken.None));
    }

    /// <summary>
    /// Executes a command and returns an enumerable of type <typeparamref name="T"/>.
    /// </summary>
    public static IEnumerable<T> Query<T>(
        this IDbCommand command,
        string commandText,
        DbParams dbParams,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text)
    {
        command.Prepare(commandText, dbParams, commandType);
        return command.Do(
            DatabaseErrorCode.CouldNotExecuteReader,
            cmd =>
            {
                using var rd = command.TryExecuteReader(commandBehavior);
                return rd.Map(map);
            });
    }

    /// <summary>
    /// Executes a command and returns an enumerable of type <typeparamref name="T"/>.
    /// </summary>
    public static IEnumerable<T> Query<T>(
        this IDbCommand command,
        string commandText,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text) =>
        command.Query(commandText, [], map, commandBehavior, commandType);

    /// <summary>
    /// Executes a command and returns an enumerable of type <typeparamref name="T"/> asynchronously.
    /// </summary>
    public static Task<IEnumerable<T>> QueryAsync<T>(
        this IDbCommand command,
        string commandText,
        DbParams dbParams,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
        {
            command.Prepare(commandText, dbParams, commandType);
            return command.DoAsync(
                DatabaseErrorCode.CouldNotExecuteReader,
                async cmd =>
                {
                    using var rd = await cmd.TryExecuteReaderAsync(commandBehavior, cancellationToken);
                    return await rd.MapAsync(map);
                });
        }

    /// <summary>
    /// Executes a command and returns an enumerable of type <typeparamref name="T"/> asynchronously.
    /// </summary>
    public static Task<IEnumerable<T>> QueryAsync<T>(
        this IDbCommand command,
        string commandText,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null) =>
        command.QueryAsync(commandText, [], map, commandBehavior, commandType, cancellationToken);

    /// <summary>
    /// Executes a command and returns a single result of type <typeparamref name="T"/>.
    /// </summary>
    public static T? QuerySingle<T>(
        this IDbCommand command,
        string commandText,
        DbParams dbParams,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text)
    {
        command.Prepare(commandText, dbParams, commandType);
        return command.Do(
            DatabaseErrorCode.CouldNotExecuteReader,
            cmd =>
            {
                using var rd = cmd.TryExecuteReader(commandBehavior);
                return rd.MapFirst(map);
            });
    }

    /// <summary>
    /// Executes a command and returns a single result of type <typeparamref name="T"/>.
    /// </summary>
    public static T? QuerySingle<T>(
        this IDbCommand command,
        string commandText,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text) =>
        command.QuerySingle(commandText, [], map, commandBehavior, commandType);

    /// <summary>
    /// Executes a command and returns a single result of type <typeparamref name="T"/> asynchronously.
    /// </summary>
    public static Task<T?> QuerySingleAsync<T>(
        this IDbCommand command,
        string commandText,
        DbParams dbParams,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        command.Prepare(commandText, dbParams, commandType);
        return command.DoAsync(
            DatabaseErrorCode.CouldNotExecuteReader,
            async cmd =>
            {
                using var rd = await cmd.TryExecuteReaderAsync(commandBehavior, cancellationToken);
                return await rd.MapFirstAsync(map, cancellationToken);
            });
    }

    /// <summary>
    /// Executes a command and returns a single result of type <typeparamref name="T"/> asynchronously.
    /// </summary>
    public static Task<T?> QuerySingleAsync<T>(
        this IDbCommand command,
        string commandText,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null) =>
        command.QuerySingleAsync(commandText, [], map, commandBehavior, commandType, cancellationToken);

    /// <summary>
    /// Executes a command, applies the <paramref name="map"/> function to the
    /// <see cref="IDataReader"/>, and returns the result of type <typeparamref name="T"/>.
    /// </summary>
    public static T Read<T>(
        this IDbCommand command,
        string commandText,
        DbParams dbParams,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text)
    {
        command.Prepare(commandText, dbParams, commandType);
        return command.Do(
            DatabaseErrorCode.CouldNotExecuteReader,
            cmd =>
            {
                using var rd = cmd.TryExecuteReader(commandBehavior);
                return map(rd);
            });
    }

    /// <summary>
    /// Executes a command, applies the <paramref name="map"/> function to the
    /// <see cref="IDataReader"/>, and returns the result of type <typeparamref name="T"/>.
    /// </summary>
    public static T Read<T>(
        this IDbCommand command,
        string commandText,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text) =>
        command.Read(commandText, [], map, commandBehavior, commandType);

    /// <summary>
    /// Executes a command, applies the <paramref name="map"/> function to the
    /// <see cref="IDataReader"/>, and returns the result of type
    /// <typeparamref name="T"/> asynchronously.
    /// </summary>
    public static Task<T> ReadAsync<T>(
        this IDbCommand command,
        string commandText,
        DbParams dbParams,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        command.Prepare(commandText, dbParams, commandType);
        return command.DoAsync(
            DatabaseErrorCode.CouldNotExecuteReader,
            async cmd =>
            {
                using var rd = await cmd.TryExecuteReaderAsync(commandBehavior, cancellationToken);
                return map(rd);
            });
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
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null) =>
        command.ReadAsync(commandText, [], map, commandBehavior, commandType, cancellationToken);

    internal static void Prepare(
        this IDbCommand command,
        string commandText,
        DbParams dbParams,
        CommandType commandType)
    {
        command.CommandType = commandType;
        command.CommandText = commandText;
        command.SetDbParams(dbParams);
    }

    internal static void SetDbParams(this IDbCommand command, DbParams param)
    {
        if (param != null)
        {
            foreach (var p in param)
            {
                var commandParam = command.CreateParameter();
                commandParam.ParameterName = p.Key;

                if (p.Value is DbTypeParam dbTypeParam)
                {
                    commandParam.DbType = dbTypeParam.DbType;
                    commandParam.Value = dbTypeParam.Value ?? DBNull.Value;
                }
                else
                {
                    commandParam.Value = p.Value ?? DBNull.Value;
                }

                command.Parameters.Add(commandParam);
            }
        }
    }

    private static T Do<T>(
        this IDbCommand command,
        DatabaseErrorCode errorCode,
        Func<IDbCommand, T> func)
    {
        command.TryOpenConnection();

        try
        {
            return func(command);
        }
        catch (Exception ex)
        {
            throw new DatabaseExecutionException(errorCode, command.CommandText, ex);
        }
    }

    private static async Task<T> DoAsync<T>(
        this IDbCommand command,
        DatabaseErrorCode errorCode,
        Func<DbCommand, Task<T>> func) =>
        await command.Do(
            errorCode,
            async cmd => await func(cmd.ToDbCommand()));

    private static void DoMany<T>(
        this IDbCommand command,
        IEnumerable<DbParams> paramList,
        Func<IDbCommand, T> func)
    {
        command.Do(
            DatabaseErrorCode.CouldNotExecuteNonQuery,
            cmd =>
            {
                foreach (var param in paramList)
                {
                    cmd.Parameters.Clear();
                    cmd.SetDbParams(param);
                    func(cmd);
                }

                return 0;
            });
    }

    private static async Task DoManyAsync<T>(
        this IDbCommand command,
        IEnumerable<DbParams> paramList,
        Func<DbCommand, Task<T>> func)
    {
        await command.DoAsync(
            DatabaseErrorCode.CouldNotExecuteNonQuery,
            async cmd =>
            {
                foreach (var param in paramList)
                {
                    cmd.Parameters.Clear();
                    cmd.SetDbParams(param);
                    await func(cmd);
                }

                return 0;
            });
    }

    private static IDataReader TryExecuteReader(this IDbCommand command, CommandBehavior commandBehavior)
    {
        command.TryOpenConnection();

        try
        {
            return command.ExecuteReader(commandBehavior);
        }
        catch (Exception ex)
        {
            throw new DatabaseExecutionException(DatabaseErrorCode.CouldNotExecuteReader, command.CommandText, ex);
        }
    }

    private static async Task<IDataReader> TryExecuteReaderAsync(
        this DbCommand command,
        CommandBehavior commandBehavior,
        CancellationToken? cancellationToken = null)
    {
        command.TryOpenConnection();

        try
        {
            return await command.ExecuteReaderAsync(commandBehavior, cancellationToken ?? CancellationToken.None);
        }
        catch (Exception ex)
        {
            throw new DatabaseExecutionException(DatabaseErrorCode.CouldNotExecuteReader, command.CommandText, ex);
        }
    }

    private static void TryOpenConnection(this IDbCommand command)
    {
        if (command.Connection is IDbConnection connection)
        {
            connection.TryOpen();
        }
        else
        {
            throw new DatabaseConnectionException();
        }
    }


    private static DbCommand ToDbCommand(this IDbCommand command)
    {
        if (command is DbCommand dbCmd)
        {
            return dbCmd;
        }
        else
        {
            throw new ArgumentException("IDbCommand must be a DbCommand", nameof(command));
        }
    }

}
