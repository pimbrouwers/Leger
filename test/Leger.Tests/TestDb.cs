namespace Leger.Tests;

using System.Data;
using System.IO;
using Microsoft.Data.Sqlite;
using Xunit;

[CollectionDefinition("TestDb")]
public class TestDbCollection : ICollectionFixture<TestDb>
{
}

public class TestDb : IDbConnectionFactory
{
    private const string _dbName = "Spiffy.Tests.db";
    private const string _connectionString = $"Data Source={_dbName}";

    public TestDb()
    {
        using var conn = CreateConnection();
        var sql = File.ReadAllText("test.sql");
        conn.Execute(sql);
    }

    public IDbConnection CreateConnection() =>
        new SqliteConnection(_connectionString);

    public static string GenerateRandomString() =>
        Path.GetRandomFileName().Replace(".", "");
}
