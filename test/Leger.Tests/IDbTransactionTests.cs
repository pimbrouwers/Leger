namespace Leger.Tests;

using System.Data;
using Xunit;

[Collection("TestDb")]
public class IDbTransactionTests(TestDb testDb) {
    [Fact]
    public void CanExecute() {
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
    public void CanScalar() {
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
    public void CanQuery() {
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
    public void CanQuerySingle() {
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
    public void ReadShouldWork() {
        var expected = TestDb.GenerateRandomString();
        var sql = "SELECT 1 AS num1, 2 AS num2";
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var dt = tran.Read(sql, rd => {
            var dt = new System.Data.DataTable();
            dt.Load(rd);
            return dt;
        });

        tran.Commit();
        Assert.Equal(2, dt.Columns.Count);
        Assert.Single(dt.Rows);
    }


    [Fact]
    public async Task CanStreamAsync() {
        var expected = TestDb.GenerateRandomString();

        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var result = new List<TestClass>();
        var stream = tran.StreamAsync(
            "SELECT @description AS description UNION SELECT 'a' UNION SELECT 'b'",
            new("description", expected),
            TestClassReader.Map);

        await foreach (var item in stream) {
            result.Add(item);
        }

        tran.Commit();

        Assert.Equal(3, result.Count);
        Assert.Contains(result, x => x.Description == expected);
    }

    [Fact]
    public async Task CanStreamAsync_NoParams() {
        var expected = TestDb.GenerateRandomString();

        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var result = new List<TestClass>();
        var stream = tran.StreamAsync(
            $"SELECT '{expected}' AS description UNION SELECT 'a' UNION SELECT 'b'", // we only bypass sql params here for testing purposes
            TestClassReader.Map);

        await foreach (var item in stream) {
            result.Add(item);
        }

        tran.Commit();

        Assert.Equal(3, result.Count);
        Assert.Contains(result, x => x.Description == expected);
    }

    [Fact]
    public void CanExecuteTranWithCommit() {
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
    public void CanExecuteTranWithRollback() {
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

    [Fact]
    public void Execute_ShouldThrowForNullCommandText() {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var exception = Assert.Throws<DatabaseExecutionException>(() => {
            tran.Execute(null!);
        });

        tran.Commit();
        Assert.Equal(DatabaseErrorCode.NoCommandText, exception.ErrorCode);
    }

    [Fact]
    public void Execute_ShouldThrowForInvalidCommandType() {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var exception = Assert.Throws<DatabaseExecutionException>(() => {
            tran.Execute("INVALID SQL;", commandType: (CommandType)999);
        });

        tran.Commit();
        Assert.Equal(DatabaseErrorCode.InvalidCommandType, exception.ErrorCode);
    }

    [Fact]
    public void Scalar_ShouldReturnNullForEmptyResult() {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var result = tran.Scalar("SELECT NULL AS value;");

        tran.Commit();
        Assert.Null(result);
    }

    [Fact]
    public async Task StreamAsync_ShouldHandleEmptyResults() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();
        using var tran = conn.CreateTransaction();

        var result = new List<TestClass>();
        var stream = tran.StreamAsync(
            "SELECT description FROM test_values WHERE 1 = 0;",
            TestClassReader.Map);

        await foreach (var item in stream) {
            result.Add(item);
        }

        tran.Commit();

        Assert.Empty(result);
    }

    [Fact]
    public void Scalar_ShouldThrowForInvalidSql() {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        Assert.Throws<DatabaseExecutionException>(() => {
            tran.Scalar("INVALID SQL;");
        });

        tran.Commit();
    }

    [Fact]
    public void Query_ShouldReturnEmptyForNoResults() {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var result = tran.Query(
            "SELECT * FROM test_values WHERE 1 = 0;",
            TestClassReader.Map);

        tran.Commit();
        Assert.Empty(result);
    }

    [Fact]
    public void QuerySingle_ShouldNotThrowForMultipleResults() {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var result = tran.QuerySingle(
            "SELECT 1 AS n UNION SELECT 2 AS n ORDER BY 1 ASC;",
            rd => rd.GetInt32(0));

        tran.Commit();
        Assert.Equal(1, result);
    }

    [Fact]
    public void QuerySingle_ShouldNotThrowForNoResults() {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var result = tran.QuerySingle(
            "SELECT 1 AS n WHERE 1 = 0;",
            rd => new { n = rd.GetInt32(0) });

        tran.Commit();
        Assert.Null(result);
    }

    [Fact]
    public void ExecuteMany_ShouldHandleEmptyParamList() {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var paramList = Enumerable.Empty<DbParams>();

        tran.ExecuteMany(
            "INSERT INTO test_values (description) VALUES (@description);",
            paramList);

        var result = tran.Query(
            "SELECT description FROM test_values WHERE description LIKE 'ExecuteMany_ShouldHandleEmptyParamList%';",
            TestClassReader.Map);

        tran.Commit();
        Assert.Empty(result);
    }

    [Fact]
    public void Read_ShouldThrowForInvalidMapping() {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var sql = "SELECT 1 AS num1, 2 AS num2";

        Assert.Throws<DatabaseExecutionException>(() => {
            tran.Read<object>(sql, rd => {
                throw new InvalidOperationException("Simulated mapping error");
            });
        });

        tran.Commit();
    }
}


[Collection("TestDb")]
public class IDbTransactionTestsAsync(TestDb testDb) {
    [Fact]
    public async Task CanExecuteAsync() {
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
    public async Task CanScalarAsync() {
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
    public async Task CanQueryAsync() {
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
    public async Task CanQuerySingleAsync() {
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
    public async Task ReadShouldWorkAsync() {
        var expected = TestDb.GenerateRandomString();
        var sql = "SELECT 1 AS num1, 2 AS num2";
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var dt = await tran.ReadAsync(sql, rd => {
            var dt = new System.Data.DataTable();
            dt.Load(rd);
            return dt;
        });

        tran.Commit();
        Assert.Equal(2, dt.Columns.Count);
        Assert.Single(dt.Rows);
    }

    [Fact]
    public async Task CanExecuteTranWithCommitAsync() {
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
    public async Task CanExecuteTranWithRollbackAsync() {
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

    [Fact]
    public async Task ExecuteAsync_ShouldThrowForNullCommandText() {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var exception = await Assert.ThrowsAsync<DatabaseExecutionException>(async () => {
            await tran.ExecuteAsync(null!);
        });

        tran.Commit();
        Assert.Equal(DatabaseErrorCode.NoCommandText, exception.ErrorCode);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldThrowForInvalidCommandType() {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var exception = await Assert.ThrowsAsync<DatabaseExecutionException>(async () => {
            await tran.ExecuteAsync("INVALID SQL;", commandType: (CommandType)999);
        });

        tran.Commit();
        Assert.Equal(DatabaseErrorCode.InvalidCommandType, exception.ErrorCode);
    }

    [Fact]
    public async Task ScalarAsync_ShouldReturnNullForEmptyResult() {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var result = await tran.ScalarAsync("SELECT NULL AS value;");

        tran.Commit();
        Assert.Null(result);
    }

    [Fact]
    public async Task ScalarAsync_ShouldThrowForInvalidSql() {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        await Assert.ThrowsAsync<DatabaseExecutionException>(async () => {
            await tran.ScalarAsync("INVALID SQL;");
        });
    }

    [Fact]
    public async Task QueryAsync_ShouldReturnEmptyForNoResults() {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var result = await tran.QueryAsync(
            "SELECT * FROM test_values WHERE 1 = 0;",
            TestClassReader.Map);

        tran.Commit();
        Assert.Empty(result);
    }

    [Fact]
    public async Task QuerySingleAsync_ShouldNotThrowForMultipleResults() {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var result = await tran.QuerySingleAsync(
            "SELECT 1 AS n UNION SELECT 2 AS n ORDER BY 1 ASC;",
            rd => rd.GetInt32(0));

        tran.Commit();
        Assert.Equal(1, result);
    }

    [Fact]
    public async Task QuerySingleAsync_ShouldNotThrowForNoResults() {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var result = await tran.QuerySingleAsync(
            "SELECT 1 AS n WHERE 1 = 0;",
            rd => new { n = rd.GetInt32(0) });

        tran.Commit();
        Assert.Null(result);
    }

    [Fact]
    public async Task ExecuteManyAsync_ShouldHandleEmptyParamList() {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var paramList = Enumerable.Empty<DbParams>();

        await tran.ExecuteManyAsync(
            "INSERT INTO test_values (description) VALUES (@description);",
            paramList);

        var result = await tran.QueryAsync(
            "SELECT description FROM test_values WHERE description LIKE 'ExecuteMany_ShouldHandleEmptyParamList%';",
            TestClassReader.Map);

        tran.Commit();
        Assert.Empty(result);
    }

    [Fact]
    public async Task ReadAsync_ShouldThrowForInvalidMapping() {
        using var conn = testDb.CreateConnection();
        using var tran = conn.CreateTransaction();

        var sql = "SELECT 1 AS num1, 2 AS num2";

        await Assert.ThrowsAsync<DatabaseExecutionException>(async () => {
            await tran.ReadAsync<object>(sql, rd => {
                throw new InvalidOperationException("Simulated mapping error");
            });
        });

        tran.Commit();
    }

    [Fact]
    public async Task ReadShouldWorkAsyncWithAsyncMap() {
        var expected = TestDb.GenerateRandomString();
        var sql = "SELECT 1 AS num UNION SELECT 2";
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();
        using var tran = conn.CreateTransaction();

        var result = await tran.ReadAsync(sql, rd => {
            return rd.MapAsync(r => r.GetInt32(0));
        });

        tran.Commit();

        Assert.Equal(2, result.Count());
    }
}
