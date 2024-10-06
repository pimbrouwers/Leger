namespace Leger;

using System.Data;

public static class IDbTransactionExtensions
{
    public static void Execute(
        this IDbTransaction transaction,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text)
    {
        using var command = transaction.CreateTextCommand(commandText, dbParams ?? [], commandType);
        command.Execute();
    }

    public static async Task ExecuteAsync(
        this IDbTransaction transaction,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var command = transaction.CreateTextCommand(commandText, dbParams ?? [], commandType);
        await command.ExecuteAsync(cancellationToken);
    }

    public static void ExecuteMany(
        this IDbTransaction transaction,
        string commandText,
        IEnumerable<DbParams> paramList,
        CommandType commandType = CommandType.Text)
    {
        using var command = transaction.CreateTextCommand(commandText, [], commandType);
        command.ExecuteMany(paramList);
    }

    public static async Task ExecuteManyAsync(
        this IDbTransaction transaction,
        string commandText,
        IEnumerable<DbParams> paramList,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var command = transaction.CreateTextCommand(commandText, [], commandType);
        await command.ExecuteManyAsync(paramList, cancellationToken);
    }

    public static object? Scalar(
        this IDbTransaction transaction,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text)
    {
        using var command = transaction.CreateTextCommand(commandText, dbParams ?? [], commandType);
        return command.Scalar();
    }

    public static async Task<object?> ScalarAsync(
        this IDbTransaction transaction,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var command = transaction.CreateTextCommand(commandText, dbParams ?? [], commandType);
        return await command.ScalarAsync(cancellationToken);
    }

    public static IEnumerable<T> Query<T>(
        this IDbTransaction transaction,
        string commandText,
        DbParams dbParams,
        Func<IDataRecord, T> map,
        CommandType commandType = CommandType.Text)
    {
        using var command = transaction.CreateTextCommand(commandText, dbParams, commandType);
        return command.Query(map);
    }

    public static IEnumerable<T> Query<T>(
        this IDbTransaction transaction,
        string commandText,
        Func<IDataRecord, T> map,
        CommandType commandType = CommandType.Text) =>
        transaction.Query(commandText, [], map, commandType);

    public static async Task<IEnumerable<T>> QueryAsync<T>(
        this IDbTransaction transaction,
        string commandText,
        DbParams dbParams,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var command = transaction.CreateTextCommand(commandText, dbParams, commandType);
        return await command.QueryAsync(map, commandBehavior, cancellationToken);
    }

    public static async Task<IEnumerable<T>> QueryAsync<T>(
        this IDbTransaction transaction,
        string commandText,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null) =>
        await transaction.QueryAsync(commandText, [], map, commandBehavior, commandType, cancellationToken);

    public static T? QuerySingle<T>(
        this IDbTransaction transaction,
        string commandText,
        DbParams dbParams,
        Func<IDataRecord, T> map,
        CommandType commandType = CommandType.Text)
    {
        using var command = transaction.CreateTextCommand(commandText, dbParams, commandType);
        return command.QuerySingle(map);
    }

    public static T? QuerySingle<T>(
        this IDbTransaction transaction,
        string commandText,
        Func<IDataRecord, T> map,
        CommandType commandType = CommandType.Text) =>
        transaction.QuerySingle(commandText, [], map, commandType);

    public static async Task<T?> QuerySingleAsync<T>(
        this IDbTransaction transaction,
        string commandText,
        DbParams dbParams,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var command = transaction.CreateTextCommand(commandText, dbParams, commandType);
        return await command.QuerySingleAsync(map, commandBehavior, cancellationToken);
    }

    public static async Task<T?> QuerySingleAsync<T>(
        this IDbTransaction transaction,
        string commandText,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null) =>
        await transaction.QuerySingleAsync(commandText, [], map, commandBehavior, commandType, cancellationToken);

    public static T Read<T>(
        this IDbTransaction transaction,
        string commandText,
        DbParams dbParams,
        Func<IDataReader, T> map,
        CommandType commandType = CommandType.Text)
    {
        using var command = transaction.CreateTextCommand(commandText, dbParams, commandType);
        return command.Read(map);
    }

    public static T Read<T>(
        this IDbTransaction transaction,
        string commandText,
        Func<IDataReader, T> map,
        CommandType commandType = CommandType.Text) =>
        transaction.Read(commandText, [], map, commandType);

    public static async Task<T> ReadAsync<T>(
        this IDbTransaction transaction,
        string commandText,
        DbParams dbParams,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var command = transaction.CreateTextCommand(commandText, dbParams, commandType);
        return await command.ReadAsync(map, commandBehavior, cancellationToken);
    }

    public static Task<T> ReadAsync<T>(
        this IDbTransaction transaction,
        string commandText,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null) =>
        transaction.ReadAsync(commandText, [], map, commandBehavior, commandType, cancellationToken);

    private static IDbCommand CreateTextCommand(
        this IDbTransaction transaction,
        string commandText,
        DbParams dbParams,
        CommandType commandType)
    {
        var command =
            transaction.Connection?.CreateTypedCommand(
                commandText,
                dbParams,
                commandType) ?? throw new DatabaseTransactionException();

        command.Transaction = transaction;
        return command;
    }
}
