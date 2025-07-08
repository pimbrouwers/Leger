namespace Leger.Tests;

using Xunit;

[Collection("TestDb")]
public class IDbTransactionTests(TestDb testDb)
{
    [Fact]
    public void CanExecute()
    {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        tran.Execute("""
            INSERT INTO test_values (description) VALUES (@description);
            """,
            new("description", TestDb.GenerateRandomString()));

        tran.Commit();
        Assert.True(true);
    }

    [Fact]
    public void CanScalar()
    {
        var expected = TestDb.GenerateRandomString();

        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var result =
            tran.Scalar(
                "SELECT @description AS description",
                new("description", expected));

        tran.Commit();
        Assert.Equal(expected, result as string);
    }

    [Fact]
    public void CanQuery()
    {
        var expected = TestDb.GenerateRandomString();

        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var result =
            tran.Query(
                "SELECT @description AS description",
                new("description", expected),
                TestClassReader.Map);

        tran.Commit();
        Assert.Equal(expected, result.First().Description);
    }

    [Fact]
    public void CanQuerySingle()
    {
        var expected = TestDb.GenerateRandomString();

        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var result =
            tran.QuerySingle(
                "SELECT @description AS description",
                new("description", expected),
                TestClassReader.Map);

        tran.Commit();
        Assert.Equal(expected, result.Description);
    }

    [Fact]
    public void ReadShouldWork()
    {
        var expected = TestDb.GenerateRandomString();
        var sql = "SELECT 1 AS num1, 2 AS num2";
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var dt = tran.Read(sql, rd =>
        {
            var dt = new System.Data.DataTable();
            dt.Load(rd);
            return dt;
        });

        tran.Commit();
        Assert.Equal(2, dt.Columns.Count);
        Assert.Single(dt.Rows);
    }

    [Fact]
    public void CanExecuteTranWithCommit()
    {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var p = new DbParams("description", TestDb.GenerateRandomString());
        tran.Execute("""
            INSERT INTO test_values (description) VALUES (@description);
            """,
            p);

        tran.Commit();

        var exists =
            conn.QuerySingle("""
            SELECT description FROM test_values WHERE description = @description;
            """,
            p,
            TestClassReader.Map);

        Assert.NotNull(exists);
    }

    [Fact]
    public void CanExecuteTranWithRollback()
    {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var p = new DbParams("description", TestDb.GenerateRandomString());
        tran.Execute("""
            INSERT INTO test_values (description) VALUES (@description);
            """,
            p);

        tran.Rollback();

        var exists =
            conn.QuerySingle("""
            SELECT description FROM test_values WHERE description = @description;
            """,
            p,
            TestClassReader.Map);

        Assert.Null(exists);
    }
}


[Collection("TestDb")]
public class IDbTransactionTestsAsync(TestDb testDb)
{
    [Fact]
    public async Task CanExecuteAsync()
    {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        await tran.ExecuteAsync("""
            INSERT INTO test_values (description) VALUES (@description);
            """,
            new("description", TestDb.GenerateRandomString()));

        tran.Commit();
        Assert.True(true);
    }

    [Fact]
    public async Task CanScalarAsync()
    {
        var expected = TestDb.GenerateRandomString();

        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var result =
            await tran.ScalarAsync(
                "SELECT @description AS description",
                new("description", expected));

        tran.Commit();
        Assert.Equal(expected, result as string);
    }

    [Fact]
    public async Task CanQueryAsync()
    {
        var expected = TestDb.GenerateRandomString();

        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var result =
            await tran.QueryAsync(
                "SELECT @description AS description",
                new("description", expected),
                TestClassReader.Map);

        tran.Commit();
        Assert.Equal(expected, result.First().Description);
    }

    [Fact]
    public async Task CanQuerySingleAsync()
    {
        var expected = TestDb.GenerateRandomString();

        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var result =
            await tran.QuerySingleAsync(
                "SELECT @description AS description",
                new("description", expected),
                TestClassReader.Map);

        tran.Commit();
        Assert.Equal(expected, result.Description);
    }

    [Fact]
    public async Task ReadShouldWorkAsync()
    {
        var expected = TestDb.GenerateRandomString();
        var sql = "SELECT 1 AS num1, 2 AS num2";
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var dt = await tran.ReadAsync(sql, rd =>
        {
            var dt = new System.Data.DataTable();
            dt.Load(rd);
            return dt;
        });

        tran.Commit();
        Assert.Equal(2, dt.Columns.Count);
        Assert.Single(dt.Rows);
    }

    [Fact]
    public async Task CanExecuteTranWithCommitAsync()
    {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var p = new DbParams("description", TestDb.GenerateRandomString());
        await tran.ExecuteAsync("""
            INSERT INTO test_values (description) VALUES (@description);
            """,
            p);

        tran.Commit();

        var exists =
            await conn.QuerySingleAsync("""
            SELECT description FROM test_values WHERE description = @description;
            """,
            p,
            TestClassReader.Map);

        Assert.NotNull(exists);
    }

    [Fact]
    public async Task CanExecuteTranWithRollbackAsync()
    {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var p = new DbParams("description", TestDb.GenerateRandomString());
        await tran.ExecuteAsync("""
            INSERT INTO test_values (description) VALUES (@description);
            """,
            p);

        tran.Rollback();

        var exists =
            await conn.QuerySingleAsync("""
            SELECT description FROM test_values WHERE description = @description;
            """,
            p,
            TestClassReader.Map);

        Assert.Null(exists);
    }
}
