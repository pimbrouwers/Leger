namespace Leger.Tests;

using Xunit;

[Collection("TestDb")]
public class IDbCommandTests(TestDb testDb)
{
    [Fact]
    public void CanExecute()
    {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        cmd.Execute("""
            INSERT INTO test_values (description) VALUES (@description);
            """,
            new("description", TestDb.GenerateRandomString()));

        Assert.True(true);
    }

    [Fact]
    public void CanScalar()
    {
        var expected = TestDb.GenerateRandomString();

        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var result =
            conn.Scalar(
                "SELECT @description AS description",
                new("description", expected));

        Assert.Equal(expected, result as string);
    }

    [Fact]
    public void CanQuery()
    {
        var expected = TestDb.GenerateRandomString();

        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var result =
            cmd.Query(
                "SELECT @description AS description",
                new("description", expected),
                TestClassReader.Map);

        Assert.Equal(expected, result.First().Description);
    }

    [Fact]
    public void CanQuerySingle()
    {
        var expected = TestDb.GenerateRandomString();

        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var result =
            cmd.QuerySingle(
                "SELECT @description AS description",
                new("description", expected),
                TestClassReader.Map);

        Assert.Equal(expected, result.Description);
    }

    [Fact]
    public void ReadShouldWork()
    {
        var expected = TestDb.GenerateRandomString();
        var sql = "SELECT 1 AS num1, 2 AS num2";
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var dt = cmd.Read(sql, rd =>
        {
            var dt = new System.Data.DataTable();
            dt.Load(rd);
            return dt;
        });

        Assert.Equal(2, dt.Columns.Count);
        Assert.Single(dt.Rows);
    }
}

[Collection("TestDb")]
public class IDbCommandTestsAsync(TestDb testDb)
{
    [Fact]
    public async Task CanExecuteAsync()
    {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        await cmd.ExecuteAsync("""
            INSERT INTO test_values (description) VALUES (@description);
            """,
            new("description", TestDb.GenerateRandomString()));

        Assert.True(true);
    }

    [Fact]
    public async Task CanScalarAsync()
    {
        var expected = TestDb.GenerateRandomString();

        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var result =
            await conn.ScalarAsync(
                "SELECT @description AS description",
                new("description", expected));

        Assert.Equal(expected, result as string);
    }

    [Fact]
    public async Task CanQueryAsync()
    {
        var expected = TestDb.GenerateRandomString();

        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var result =
            await cmd.QueryAsync(
                "SELECT @description AS description",
                new("description", expected),
                TestClassReader.Map);

        Assert.Equal(expected, result.First().Description);
    }

    [Fact]
    public async Task CanQuerySingleAsync()
    {
        var expected = TestDb.GenerateRandomString();

        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var result =
            await cmd.QuerySingleAsync(
                "SELECT @description AS description",
                new("description", expected),
                TestClassReader.Map);

        Assert.Equal(expected, result.Description);
    }

    [Fact]
    public async Task ReadShouldWorkAsync()
    {
        var expected = TestDb.GenerateRandomString();
        var sql = "SELECT 1 AS num1, 2 AS num2";
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var dt = await cmd.ReadAsync(sql, rd =>
        {
            var dt = new System.Data.DataTable();
            dt.Load(rd);
            return dt;
        });

        Assert.Equal(2, dt.Columns.Count);
        Assert.Single(dt.Rows);
    }
}
