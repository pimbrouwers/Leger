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
        using var command = connection.CreateTypedCommand(commandText, dbParams ?? [], commandType);
        command.Execute();
    }

    public static async Task ExecuteAsync(
        this IDbConnection connection,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var command = connection.CreateTypedCommand(commandText, dbParams ?? [], commandType);
        await command.ExecuteAsync(cancellationToken);
    }

    public static void ExecuteMany(
        this IDbConnection connection,
        string commandText,
        IEnumerable<DbParams> paramList,
        CommandType commandType = CommandType.Text)
    {
        using var command = connection.CreateTypedCommand(commandText, [], commandType);
        command.ExecuteMany(paramList);
    }

    public static async Task ExecuteManyAsync(
        this IDbConnection connection,
        string commandText,
        IEnumerable<DbParams> paramList,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var command = connection.CreateTypedCommand(commandText, [], commandType);
        await command.ExecuteManyAsync(paramList, cancellationToken);
    }

    public static object? Scalar(
        this IDbConnection connection,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text)
    {
        using var command = connection.CreateTypedCommand(commandText, dbParams ?? [], commandType);
        return command.Scalar();
    }

    public static async Task<object?> ScalarAsync(
        this IDbConnection connection,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var command = connection.CreateTypedCommand(commandText, dbParams ?? [], commandType);
        return await command.ScalarAsync(cancellationToken);
    }

    public static IEnumerable<T> Query<T>(
        this IDbConnection connection,
        string commandText,
        DbParams dbParams,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text)
    {
        using var command = connection.CreateTypedCommand(commandText, dbParams, commandType);
        return command.Query(map, commandBehavior);
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
        using var command = connection.CreateTypedCommand(commandText, dbParams, commandType);
        return await command.QueryAsync(map, commandBehavior, cancellationToken);
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
        using var command = connection.CreateTypedCommand(commandText, dbParams, commandType);
        return command.QuerySingle(map, commandBehavior);
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
        using var command = connection.CreateTypedCommand(commandText, dbParams, commandType);
        return await command.QuerySingleAsync(map, commandBehavior, cancellationToken);
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
        using var command = connection.CreateTypedCommand(commandText, dbParams, commandType);
        return command.Read(map, commandBehavior);
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
        using var command = connection.CreateTypedCommand(commandText, dbParams, commandType);
        return await command.ReadAsync(map, commandBehavior, cancellationToken);
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

    internal static IDbCommand CreateTypedCommand(
        this IDbConnection connection,
        string commandText,
        DbParams dbParams,
        CommandType commandType)
    {
        var command = connection.CreateCommand();
        command.CommandType = commandType;
        command.CommandText = commandText;
        command.SetDbParams(dbParams);
        return command;
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
