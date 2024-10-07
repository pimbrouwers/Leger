namespace Leger;

using System.Data;

public static class IDbConnectionExtensions
{
    public static void Execute(
        this IDbConnection connection,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text)
    {
        using var command = connection.CreateCommand();
        command.Execute(commandText, dbParams, commandType);
    }

    public static async Task ExecuteAsync(
        this IDbConnection connection,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var command = connection.CreateCommand();
        await command.ExecuteAsync(commandText, dbParams ?? [], commandType, cancellationToken);
    }

    public static void ExecuteMany(
        this IDbConnection connection,
        string commandText,
        IEnumerable<DbParams> paramList,
        CommandType commandType = CommandType.Text)
    {
        using var command = connection.CreateCommand();
        command.ExecuteMany(commandText, paramList, commandType);
    }

    public static async Task ExecuteManyAsync(
        this IDbConnection connection,
        string commandText,
        IEnumerable<DbParams> paramList,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var command = connection.CreateCommand();
        await command.ExecuteManyAsync(commandText, paramList, commandType, cancellationToken);
    }

    public static object? Scalar(
        this IDbConnection connection,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text)
    {
        using var command = connection.CreateCommand();
        return command.Scalar(commandText, dbParams ?? [], commandType);
    }

    public static async Task<object?> ScalarAsync(
        this IDbConnection connection,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var command = connection.CreateCommand();
        return await command.ScalarAsync(commandText, dbParams ?? [], commandType);
    }

    public static IEnumerable<T> Query<T>(
        this IDbConnection connection,
        string commandText,
        DbParams dbParams,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text)
    {
        using var command = connection.CreateCommand();
        return command.Query(commandText, dbParams, map, commandBehavior, commandType);
    }

    public static IEnumerable<T> Query<T>(
        this IDbConnection connection,
        string commandText,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text) =>
        connection.Query(commandText, [], map, commandBehavior, commandType);

    public static async Task<IEnumerable<T>> QueryAsync<T>(
        this IDbConnection connection,
        string commandText,
        DbParams dbParams,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var command = connection.CreateCommand();
        return await command.QueryAsync(commandText, dbParams, map, commandBehavior, commandType, cancellationToken);
    }

    public static async Task<IEnumerable<T>> QueryAsync<T>(
        this IDbConnection connection,
        string commandText,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null) =>
        await connection.QueryAsync(commandText, [], map, commandBehavior, commandType, cancellationToken);

    public static T? QuerySingle<T>(
        this IDbConnection connection,
        string commandText,
        DbParams dbParams,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text)
    {
        using var command = connection.CreateCommand();
        return command.QuerySingle(commandText, dbParams, map, commandBehavior, commandType);
    }

    public static T? QuerySingle<T>(
        this IDbConnection connection,
        string commandText,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text) =>
        connection.QuerySingle(commandText, [], map, commandBehavior, commandType);

    public static async Task<T?> QuerySingleAsync<T>(
        this IDbConnection connection,
        string commandText,
        DbParams dbParams,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var command = connection.CreateCommand();
        return await command.QuerySingleAsync(commandText, dbParams, map, commandBehavior, commandType, cancellationToken);
    }

    public static async Task<T?> QuerySingleAsync<T>(
        this IDbConnection connection,
        string commandText,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null) =>
        await connection.QuerySingleAsync(commandText, [], map, commandBehavior, commandType, cancellationToken);

    public static T Read<T>(
        this IDbConnection connection,
        string commandText,
        DbParams dbParams,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text)
    {
        using var command = connection.CreateCommand();
        return command.Read(commandText, dbParams, map, commandBehavior, commandType);
    }

    public static T Read<T>(
        this IDbConnection connection,
        string commandText,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text) =>
        connection.Read(commandText, [], map, commandBehavior, commandType);

    public static async Task<T> ReadAsync<T>(
        this IDbConnection connection,
        string commandText,
        DbParams dbParams,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var command = connection.CreateCommand();
        return await command.ReadAsync(commandText, dbParams, map, commandBehavior, commandType, cancellationToken);
    }

    public static Task<T> ReadAsync<T>(
        this IDbConnection connection,
        string commandText,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null) =>
        connection.ReadAsync(commandText, [], map, commandBehavior, commandType, cancellationToken);

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
