namespace Leger.Tests;

using System.Data;
using System.IO;
using Microsoft.Data.Sqlite;
using Xunit;

public class TestDb : IDbConnectionFactory
{
    private const string DbName = "Spiffy.Tests.db";
    private const string ConnectionString = $"Data Source={DbName}";

    public TestDb()
    {
        using var conn = CreateConnection();
        var sql = File.ReadAllText("test.sql");
        conn.Execute(sql);
    }

    public IDbConnection CreateConnection() =>
        new SqliteConnection(ConnectionString);

    public static string GenerateRandomString() =>
        Path.GetRandomFileName().Replace(".", "");
}

[CollectionDefinition("TestDb")]
public class TestDbCollection : ICollectionFixture<TestDb>
{
}

public class TestClass()
{
    public TestClass(string description) : this()
    {
        Description = description;
    }

    public string Description { get; set; } = string.Empty;
}

public static class TestClassReader
{
    public static TestClass Map(this IDataReader rd) =>
        new(rd.ReadString("description"));
}
