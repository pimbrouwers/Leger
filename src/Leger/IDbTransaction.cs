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
        using var command = transaction.CreateTransactionCommand();
        command.Execute(commandText, dbParams, commandType);
    }

    public static async Task ExecuteAsync(
        this IDbTransaction transaction,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var command = transaction.CreateTransactionCommand();
        await command.ExecuteAsync(commandText, dbParams, commandType, cancellationToken);
    }

    public static void ExecuteMany(
        this IDbTransaction transaction,
        string commandText,
        IEnumerable<DbParams> paramList,
        CommandType commandType = CommandType.Text)
    {
        using var command = transaction.CreateTransactionCommand();
        command.ExecuteMany(commandText, paramList, commandType);
    }

    public static async Task ExecuteManyAsync(
        this IDbTransaction transaction,
        string commandText,
        IEnumerable<DbParams> paramList,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var command = transaction.CreateTransactionCommand();
        await command.ExecuteManyAsync(commandText, paramList, commandType, cancellationToken);
    }

    public static object? Scalar(
        this IDbTransaction transaction,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text)
    {
        using var command = transaction.CreateTransactionCommand();
        return command.Scalar(commandText, dbParams ?? [], commandType);
    }

    public static async Task<object?> ScalarAsync(
        this IDbTransaction transaction,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var command = transaction.CreateTransactionCommand();
        return await command.ScalarAsync(commandText, dbParams ?? [], commandType, cancellationToken);
    }

    public static IEnumerable<T> Query<T>(
        this IDbTransaction transaction,
        string commandText,
        DbParams dbParams,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text)
    {
        using var command = transaction.CreateTransactionCommand();
        return command.Query(commandText, dbParams, map, commandBehavior, commandType);
    }

    public static IEnumerable<T> Query<T>(
        this IDbTransaction transaction,
        string commandText,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text) =>
        transaction.Query(commandText, [], map, commandBehavior, commandType);

    public static async Task<IEnumerable<T>> QueryAsync<T>(
        this IDbTransaction transaction,
        string commandText,
        DbParams dbParams,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var command = transaction.CreateTransactionCommand();
        return await command.QueryAsync(commandText, dbParams, map, commandBehavior, commandType, cancellationToken);
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
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text)
    {
        using var command = transaction.CreateTransactionCommand();
        return command.QuerySingle(commandText, dbParams, map, commandBehavior, commandType);
    }

    public static T? QuerySingle<T>(
        this IDbTransaction transaction,
        string commandText,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text) =>
        transaction.QuerySingle(commandText, [], map, commandBehavior, commandType);

    public static async Task<T?> QuerySingleAsync<T>(
        this IDbTransaction transaction,
        string commandText,
        DbParams dbParams,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var command = transaction.CreateTransactionCommand();
        return await command.QuerySingleAsync(commandText, dbParams, map, commandBehavior, commandType, cancellationToken);
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
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text)
    {
        using var command = transaction.CreateTransactionCommand();
        return command.Read(commandText, dbParams, map, commandBehavior, commandType);
    }

    public static T Read<T>(
        this IDbTransaction transaction,
        string commandText,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text) =>
        transaction.Read(commandText, [], map, commandBehavior, commandType);

    public static async Task<T> ReadAsync<T>(
        this IDbTransaction transaction,
        string commandText,
        DbParams dbParams,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var command = transaction.CreateTransactionCommand();
        return await command.ReadAsync(commandText, dbParams, map, commandBehavior, commandType, cancellationToken);
    }

    public static Task<T> ReadAsync<T>(
        this IDbTransaction transaction,
        string commandText,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null) =>
        transaction.ReadAsync(commandText, [], map, commandBehavior, commandType, cancellationToken);

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
