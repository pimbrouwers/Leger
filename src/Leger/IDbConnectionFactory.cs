namespace Leger;

using System.Data;

public interface IDbConnectionFactory
{
    IDbConnection CreateConnection();
}

public static class IDbConnectionFactoryExtensions
{
    public static void Execute(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text)
    {
        using var connection = connectionFactory.CreateConnection();
        connection.Execute(commandText, dbParams, commandType);
    }

    public static async Task ExecuteAsync(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var connection = connectionFactory.CreateConnection();
        await connection.ExecuteAsync(commandText, dbParams, commandType, cancellationToken);
    }

    public static void ExecuteMany(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        IEnumerable<DbParams> paramList,
        CommandType commandType = CommandType.Text)
    {
        using var connection = connectionFactory.CreateConnection();
        connection.ExecuteMany(commandText, paramList, commandType);
    }

    public static async Task ExecuteManyAsync(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        IEnumerable<DbParams> paramList,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var connection = connectionFactory.CreateConnection();
        await connection.ExecuteManyAsync(commandText, paramList, commandType, cancellationToken);
    }

    public static object? Scalar(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text)
    {
        using var connection = connectionFactory.CreateConnection();
        return connection.Scalar(commandText, dbParams, commandType);
    }

    public static async Task<object?> ScalarAsync(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var connection = connectionFactory.CreateConnection();
        return await connection.ScalarAsync(commandText, dbParams, commandType, cancellationToken);
    }

    public static IEnumerable<T> Query<T>(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        DbParams dbParams,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text)
    {
        using var connection = connectionFactory.CreateConnection();
        return connection.Query(commandText, dbParams, map, commandBehavior, commandType);
    }

    public static IEnumerable<T> Query<T>(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text)
    {
        using var connection = connectionFactory.CreateConnection();
        return connection.Query(commandText, map, commandBehavior, commandType);
    }

    public static async Task<IEnumerable<T>> QueryAsync<T>(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        DbParams dbParams,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var connection = connectionFactory.CreateConnection();
        return await connection.QueryAsync(commandText, dbParams, map, commandBehavior, commandType, cancellationToken);
    }

    public static async Task<IEnumerable<T>> QueryAsync<T>(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var connection = connectionFactory.CreateConnection();
        return await connection.QueryAsync(commandText, map, commandBehavior, commandType, cancellationToken);
    }

    public static T? QuerySingle<T>(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        DbParams dbParams,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text)
    {
        using var connection = connectionFactory.CreateConnection();
        return connection.QuerySingle(commandText, dbParams, map, commandBehavior, commandType);
    }

    public static T? QuerySingle<T>(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text)
    {
        using var connection = connectionFactory.CreateConnection();
        return connection.QuerySingle(commandText, map, commandBehavior, commandType);
    }

    public static async Task<T?> QuerySingleAsync<T>(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        DbParams dbParams,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var connection = connectionFactory.CreateConnection();
        return await connection.QuerySingleAsync(commandText, dbParams, map, commandBehavior, commandType, cancellationToken);
    }

    public static async Task<T?> QuerySingleAsync<T>(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        Func<IDataRecord, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var connection = connectionFactory.CreateConnection();
        return await connection.QuerySingleAsync(commandText, map, commandBehavior, commandType, cancellationToken);
    }

    public static T Read<T>(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        DbParams dbParams,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text)
    {
        using var connection = connectionFactory.CreateConnection();
        return connection.Read(commandText, dbParams, map, commandBehavior, commandType);
    }

    public static T Read<T>(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text) =>
        connectionFactory.Read(commandText, [], map, commandBehavior, commandType);

    public static async Task<T> ReadAsync<T>(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        DbParams dbParams,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var connection = connectionFactory.CreateConnection();
        return await connection.ReadAsync(commandText, dbParams, map, commandBehavior, commandType, cancellationToken);
    }

    public static Task<T> ReadAsync<T>(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null) =>
        connectionFactory.ReadAsync(commandText, [], map, commandBehavior, commandType, cancellationToken);
}
