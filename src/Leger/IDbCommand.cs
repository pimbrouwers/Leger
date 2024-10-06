﻿namespace Leger;

using System.Data;
using System.Data.Common;

public static class IDbCommandExtensions
{
    public static void Execute(
        this IDbCommand command)
    {
        command.Do(
            DatabaseErrorCode.CouldNotExecuteNonQuery,
            cmd => cmd.ExecuteNonQuery());
    }

    public static Task ExecuteAsync(
        this IDbCommand command,
        CancellationToken? cancellationToken = null) =>
        command.DoAsync(
            DatabaseErrorCode.CouldNotExecuteNonQuery,
            cmd => cmd.ExecuteNonQueryAsync(cancellationToken ?? CancellationToken.None));

    public static void ExecuteMany(
        this IDbCommand command,
        IEnumerable<DbParams> paramList) =>
      command.DoMany(paramList, command => command.ExecuteNonQuery());

    public static async Task ExecuteManyAsync(
        this IDbCommand command,
        IEnumerable<DbParams> paramList,
        CancellationToken? cancellationToken = null) =>
        await command.DoManyAsync(paramList, command =>
            command.ExecuteNonQueryAsync(cancellationToken ?? CancellationToken.None));

    public static object? Scalar(
        this IDbCommand command) =>
        command.Do(
            DatabaseErrorCode.CouldNotExecuteScalar,
            cmd => cmd.ExecuteScalar());

    public static Task<object?> ScalarAsync(
        this IDbCommand command,
        CancellationToken? cancellationToken = null) =>
        command.DoAsync(
            DatabaseErrorCode.CouldNotExecuteScalar,
            cmd => cmd.ExecuteScalarAsync(cancellationToken ?? CancellationToken.None));

    public static IEnumerable<T> Query<T>(
        this IDbCommand command,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess) =>
        command.Do(
            DatabaseErrorCode.CouldNotExecuteReader,
            cmd =>
            {
                using var rd = command.TryExecuteReader(commandBehavior);
                return rd.Map(map);
            });

    public static Task<IEnumerable<T>> QueryAsync<T>(
        this IDbCommand command,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CancellationToken? cancellationToken = null) =>
        command.DoAsync(
            DatabaseErrorCode.CouldNotExecuteReader,
            async cmd =>
            {
                using var rd = await cmd.TryExecuteReaderAsync(commandBehavior, cancellationToken);
                return await rd.MapAsync(map);
            });

    public static T? QuerySingle<T>(
        this IDbCommand command,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess) =>
        command.Do(
            DatabaseErrorCode.CouldNotExecuteReader,
            cmd =>
            {
                using var rd = cmd.TryExecuteReader(commandBehavior);
                return rd.MapFirst(map);
            });

    public static Task<T?> QuerySingleAsync<T>(
        this IDbCommand command,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CancellationToken? cancellationToken = null) =>
        command.DoAsync(
            DatabaseErrorCode.CouldNotExecuteReader,
            async cmd =>
            {
                using var rd = await cmd.TryExecuteReaderAsync(commandBehavior, cancellationToken);
                return await rd.MapFirstAsync(map, cancellationToken);
            });

    public static T Read<T>(
        this IDbCommand command,
        Func<IDataReader, T> read,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess) =>
        command.Do(
            DatabaseErrorCode.CouldNotExecuteReader,
            cmd =>
            {
                using var rd = cmd.TryExecuteReader(commandBehavior);
                return read(rd);
            });

    public static Task<T> ReadAsync<T>(
        this IDbCommand command,
        Func<IDataReader, T> read,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CancellationToken? cancellationToken = null) =>
        command.DoAsync(
            DatabaseErrorCode.CouldNotExecuteReader,
            async cmd =>
            {
                using var rd = await cmd.TryExecuteReaderAsync(commandBehavior, cancellationToken);
                return read(rd);
            });

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