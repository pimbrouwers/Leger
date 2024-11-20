namespace Leger;

using System.Data;

/// <summary>
/// Represents a factory for creating <see cref="IDbConnection"/> instances.
/// </summary>
public interface IDbConnectionFactory
{
    /// <summary> Creates and opens a connection to the database. </summary>
    IDbConnection CreateConnection();
}

/// <summary>
/// IDbConnectionFactory extensions.
/// </summary>
public static class IDbConnectionFactoryExtensions
{
    /// <summary>
    /// Executes a command.
    /// </summary>
    public static void Execute(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text)
    {
        using var connection = connectionFactory.CreateConnection();
        connection.Execute(commandText, dbParams, commandType);
    }

    /// <summary>
    /// Executes a command asynchronously.
    /// </summary>
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

    /// <summary>
    /// Executes a command many times.
    /// </summary>
    public static void ExecuteMany(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        IEnumerable<DbParams> paramList,
        CommandType commandType = CommandType.Text)
    {
        using var connection = connectionFactory.CreateConnection();
        connection.ExecuteMany(commandText, paramList, commandType);
    }

    /// <summary>
    /// Executes a command many times asynchronously.
    /// </summary>
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

    /// <summary>
    /// Executes a command and returns a scalar.
    /// </summary>
    public static object? Scalar(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        DbParams? dbParams = null,
        CommandType commandType = CommandType.Text)
    {
        using var connection = connectionFactory.CreateConnection();
        return connection.Scalar(commandText, dbParams, commandType);
    }

    /// <summary>
    /// Executes a command and returns a scalar asynchronously.
    /// </summary>
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

    /// <summary>
    /// Executes a command and returns an enumerable of type <typeparamref name="T"/>.
    /// </summary>
    public static IEnumerable<T> Query<T>(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        DbParams dbParams,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text)
    {
        using var connection = connectionFactory.CreateConnection();
        return connection.Query(commandText, dbParams, map, commandBehavior, commandType);
    }

    /// <summary>
    /// Executes a command and returns an enumerable of type <typeparamref name="T"/>.
    /// </summary>
    public static IEnumerable<T> Query<T>(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text)
    {
        using var connection = connectionFactory.CreateConnection();
        return connection.Query(commandText, map, commandBehavior, commandType);
    }

    /// <summary>
    /// Executes a command and returns an enumerable of type <typeparamref name="T"/> asynchronously.
    /// </summary>
    public static async Task<IEnumerable<T>> QueryAsync<T>(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        DbParams dbParams,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var connection = connectionFactory.CreateConnection();
        return await connection.QueryAsync(commandText, dbParams, map, commandBehavior, commandType, cancellationToken);
    }

    /// <summary>
    /// Executes a command and returns an enumerable of type <typeparamref name="T"/> asynchronously.
    /// </summary>
    public static async Task<IEnumerable<T>> QueryAsync<T>(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var connection = connectionFactory.CreateConnection();
        return await connection.QueryAsync(commandText, map, commandBehavior, commandType, cancellationToken);
    }

    /// <summary>
    /// Executes a command and returns a single result of type <typeparamref name="T"/>.
    /// </summary>
    public static T? QuerySingle<T>(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        DbParams dbParams,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text)
    {
        using var connection = connectionFactory.CreateConnection();
        return connection.QuerySingle(commandText, dbParams, map, commandBehavior, commandType);
    }

    /// <summary>
    /// Executes a command and returns a single result of type <typeparamref name="T"/>.
    /// </summary>
    public static T? QuerySingle<T>(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text)
    {
        using var connection = connectionFactory.CreateConnection();
        return connection.QuerySingle(commandText, map, commandBehavior, commandType);
    }

    /// <summary>
    /// Executes a command and returns a single result of type <typeparamref name="T"/> asynchronously.
    /// </summary>
    public static async Task<T?> QuerySingleAsync<T>(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        DbParams dbParams,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var connection = connectionFactory.CreateConnection();
        return await connection.QuerySingleAsync(commandText, dbParams, map, commandBehavior, commandType, cancellationToken);
    }

    /// <summary>
    /// Executes a command and returns a single result of type <typeparamref name="T"/> asynchronously.
    /// </summary>
    public static async Task<T?> QuerySingleAsync<T>(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null)
    {
        using var connection = connectionFactory.CreateConnection();
        return await connection.QuerySingleAsync(commandText, map, commandBehavior, commandType, cancellationToken);
    }

    /// <summary>
    /// Executes a command, applies the <paramref name="map"/> function to the
    /// <see cref="IDataReader"/>, and returns the result of type <typeparamref name="T"/>.
    /// </summary>
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

    /// <summary>
    /// Executes a command, applies the <paramref name="map"/> function to the
    /// <see cref="IDataReader"/>, and returns the result of type <typeparamref name="T"/>.
    /// </summary>
    public static T Read<T>(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text) =>
        connectionFactory.Read(commandText, [], map, commandBehavior, commandType);

    /// <summary>
    /// Executes a command, applies the <paramref name="map"/> function to the
    /// <see cref="IDataReader"/>, and returns the result of type
    /// <typeparamref name="T"/> asynchronously.
    /// </summary>
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

    /// <summary>
    /// Executes a command, applies the <paramref name="map"/> function to the
    /// <see cref="IDataReader"/>, and returns the result of type
    /// <typeparamref name="T"/> asynchronously.
    /// </summary>
    public static Task<T> ReadAsync<T>(
        this IDbConnectionFactory connectionFactory,
        string commandText,
        Func<IDataReader, T> map,
        CommandBehavior commandBehavior = CommandBehavior.SequentialAccess,
        CommandType commandType = CommandType.Text,
        CancellationToken? cancellationToken = null) =>
        connectionFactory.ReadAsync(commandText, [], map, commandBehavior, commandType, cancellationToken);
}
