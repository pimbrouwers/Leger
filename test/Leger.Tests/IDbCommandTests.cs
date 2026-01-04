namespace Leger.Tests;

using System.Data;
using Xunit;

[Collection("TestDb")]
public class IDbCommandTests(TestDb testDb) {
    [Fact]
    public void CanExecute() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        cmd.Execute("""
            INSERT INTO test_values (description) VALUES (@description);
            """,
            new("description", TestDb.GenerateRandomString()));

        Assert.True(true);
    }

    [Fact]
    public void CanScalar() {
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
    public void CanQuery() {
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
    public void CanQuerySingle() {
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
    public void ReadShouldWork() {
        var expected = TestDb.GenerateRandomString();
        var sql = "SELECT 1 AS num1, 2 AS num2";
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var dt = cmd.Read(sql, rd => {
            var dt = new DataTable();
            dt.Load(rd);
            return dt;
        });

        Assert.Equal(2, dt.Columns.Count);
        Assert.Single(dt.Rows);
    }

    [Fact]
    public void Execute_ShouldThrowForNullCommandText() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var exception = Assert.Throws<DatabaseExecutionException>(() => {
            cmd.Execute(null!);
        });

        Assert.Equal(DatabaseErrorCode.NoCommandText, exception.ErrorCode);
    }

    [Fact]
    public void Execute_ShouldThrowForInvalidCommandType() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var exception = Assert.Throws<DatabaseExecutionException>(() => {
            cmd.Execute("INVALID SQL;", commandType: (CommandType)999);
        });

        Assert.Equal(DatabaseErrorCode.InvalidCommandType, exception.ErrorCode);
    }

    [Fact]
    public void Scalar_ShouldReturnNullForEmptyResult() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var result = cmd.Scalar("SELECT NULL AS value;");
        Assert.Null(result);
    }

    [Fact]
    public void Scalar_ShouldThrowForInvalidSql() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        Assert.Throws<DatabaseExecutionException>(() => {
            cmd.Scalar("INVALID SQL;");
        });
    }

    [Fact]
    public void Query_ShouldReturnEmptyForNoResults() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var result = cmd.Query(
            "SELECT * FROM test_values WHERE 1 = 0;",
            TestClassReader.Map);

        Assert.Empty(result);
    }

    [Fact]
    public void QuerySingle_ShouldNotThrowForMultipleResults() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var result = cmd.QuerySingle(
            "SELECT 1 AS n UNION SELECT 2 AS n ORDER BY 1 ASC;",
            rd => rd.GetInt32(0));

        Assert.Equal(1, result);
    }

    [Fact]
    public void QuerySingle_ShouldNotThrowForNoResults() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var result = cmd.QuerySingle(
            "SELECT 1 AS n WHERE 1 = 0;",
            rd => new { n = rd.GetInt32(0) });

        Assert.Null(result);
    }

    [Fact]
    public void ExecuteMany_ShouldHandleEmptyParamList() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var paramList = Enumerable.Empty<DbParams>();

        cmd.ExecuteMany(
            "INSERT INTO test_values (description) VALUES (@description);",
            paramList);

        var result = cmd.Query(
            "SELECT description FROM test_values WHERE description LIKE 'ExecuteMany_ShouldHandleEmptyParamList%';",
            TestClassReader.Map);

        Assert.Empty(result);
    }

    [Fact]
    public void Read_ShouldThrowForInvalidMapping() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var sql = "SELECT 1 AS num1, 2 AS num2";

        Assert.Throws<DatabaseExecutionException>(() => {
            cmd.Read<object>(sql, rd => {
                throw new InvalidOperationException("Simulated mapping error");
            });
        });
    }

    [Fact]
    public void Execute_ShouldNotThrowForClosedConnection() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        conn.Close();

        cmd.Execute("INSERT INTO test_values (description) VALUES ('Execute_ShouldNotThrowForClosedConnection');");

        var result = cmd.Scalar("SELECT description FROM test_values WHERE description = 'Execute_ShouldNotThrowForClosedConnection';");

        Assert.Equal("Execute_ShouldNotThrowForClosedConnection", result as string);
    }

    [Fact]
    public void Scalar_ShouldNotThrowForClosedConnection() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        conn.Close();

        var result = cmd.Scalar("""
            SELECT 'Scalar_ShouldNotThrowForClosedConnection' AS description;
            """);

        Assert.Equal("Scalar_ShouldNotThrowForClosedConnection", result as string);
    }

    [Fact]
    public void Query_ShouldNotThrowForClosedConnection() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        conn.Close();

        var result = cmd.Query(
            "SELECT 'Query_ShouldNotThrowForClosedConnection' AS description;",
            TestClassReader.Map);

        Assert.Single(result);
        Assert.Equal("Query_ShouldNotThrowForClosedConnection", result.First().Description);
    }

    [Fact]
    public void QuerySingle_ShouldNotThrowForClosedConnection() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        conn.Close();

        var result = cmd.QuerySingle(
            "SELECT 'QuerySingle_ShouldNotThrowForClosedConnection' AS description;",
            TestClassReader.Map);

        Assert.Equal("QuerySingle_ShouldNotThrowForClosedConnection", result.Description);
    }
}

[Collection("TestDb")]
public class IDbCommandTestsAsync(TestDb testDb) {
    [Fact]
    public async Task CanExecuteAsync() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        await cmd.ExecuteAsync("""
            INSERT INTO test_values (description) VALUES (@description);
            """,
            new("description", TestDb.GenerateRandomString()));

        Assert.True(true);
    }

    [Fact]
    public async Task CanScalarAsync() {
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
    public async Task CanQueryAsync() {
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
    public async Task CanQuerySingleAsync() {
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
    public async Task ReadShouldWorkAsync() {
        var expected = TestDb.GenerateRandomString();
        var sql = "SELECT 1 AS num1, 2 AS num2";
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var dt = await cmd.ReadAsync(sql, rd => {
            var dt = new DataTable();
            dt.Load(rd);
            return dt;
        });

        Assert.Equal(2, dt.Columns.Count);
        Assert.Single(dt.Rows);
    }

    [Fact]
    public async Task ReadShouldWorkAsyncWithAsyncMap() {
        var expected = TestDb.GenerateRandomString();
        var sql = "SELECT 1 AS num UNION SELECT 2";
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var result = await cmd.ReadAsync(sql, rd => {
            return rd.MapAsync(r => r.GetInt32(0));
        });

        Assert.Equal(2, result.Count());
    }

    [Fact]
    public async Task CanStreamAsync() {
        var expected = TestDb.GenerateRandomString();

        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();


        var result = new List<TestClass>();
        var stream = cmd.StreamAsync(
            "SELECT @description AS description UNION SELECT 'a' UNION SELECT 'b'",
            new("description", expected),
            TestClassReader.Map);

        await foreach (var item in stream) {
            result.Add(item);
        }

        Assert.Equal(3, result.Count);
        Assert.Contains(result, x => x.Description == expected);
    }

    [Fact]
    public async Task CanStreamAsync_NoParams() {
        var expected = TestDb.GenerateRandomString();

        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();


        var result = new List<TestClass>();
        var stream = cmd.StreamAsync(
            $"SELECT '{expected}' AS description UNION SELECT 'a' UNION SELECT 'b'", // we only bypass sql params here for testing purposes
            TestClassReader.Map);

        await foreach (var item in stream) {
            result.Add(item);
        }

        Assert.Equal(3, result.Count);
        Assert.Contains(result, x => x.Description == expected);
    }

    [Fact]
    public void ExecuteMany_ShouldInsertMultipleRows() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var paramList = new[]
        {
            new DbParams { { "description", "Test 1" } },
            new DbParams { { "description", "Test 2" } },
            new DbParams { { "description", "Test 3" } }
        };

        cmd.ExecuteMany(
            "INSERT INTO test_values (description) VALUES (@description);",
            paramList);

        var result = cmd.Query(
            "SELECT description FROM test_values;",
            TestClassReader.Map);

        Assert.True(result.Count() >= 3);
        Assert.Contains(result, x => x.Description == "Test 1");
        Assert.Contains(result, x => x.Description == "Test 2");
        Assert.Contains(result, x => x.Description == "Test 3");
    }

    [Fact]
    public async Task ExecuteManyAsync_ShouldInsertMultipleRows() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var paramList = new[]
        {
            new DbParams { { "description", "Async Test 1" } },
            new DbParams { { "description", "Async Test 2" } },
            new DbParams { { "description", "Async Test 3" } }
        };

        await cmd.ExecuteManyAsync(
            "INSERT INTO test_values (description) VALUES (@description);",
            paramList);

        var result = await cmd.QueryAsync(
            "SELECT description FROM test_values;",
            TestClassReader.Map);

        Assert.True(result.Count() >= 3);
        Assert.Contains(result, x => x.Description == "Async Test 1");
        Assert.Contains(result, x => x.Description == "Async Test 2");
        Assert.Contains(result, x => x.Description == "Async Test 3");
    }

    [Fact]
    public void Query_ShouldReturnResultsWithoutParams() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var result = cmd.Query(
            "SELECT 'No Params' AS description;",
            TestClassReader.Map);

        Assert.Single(result);
        Assert.Equal("No Params", result.First().Description);
    }

    [Fact]
    public async Task QueryAsync_ShouldReturnResultsWithoutParams() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var result = await cmd.QueryAsync(
            "SELECT 'No Params Async' AS description;",
            TestClassReader.Map);

        Assert.Single(result);
        Assert.Equal("No Params Async", result.First().Description);
    }

    [Fact]
    public void QuerySingle_ShouldReturnSingleResultWithoutParams() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var result = cmd.QuerySingle(
            "SELECT 'Single Result' AS description;",
            TestClassReader.Map);

        Assert.Equal("Single Result", result.Description);
    }

    [Fact]
    public async Task QuerySingleAsync_ShouldReturnSingleResultWithoutParams() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var result = await cmd.QuerySingleAsync(
            "SELECT 'Single Result Async' AS description;",
            TestClassReader.Map);

        Assert.Equal("Single Result Async", result.Description);
    }

    [Fact]
    public void Read_ShouldMapDataCorrectly() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var sql = "SELECT 1 AS num1, 2 AS num2";
        var result = cmd.Read(sql, rd => {
            var dt = new DataTable();
            dt.Load(rd);
            return dt;
        });

        Assert.Equal(2, result.Columns.Count);
        Assert.Single(result.Rows);
        Assert.Equal(1L, (long)result.Rows[0]["num1"]);
        Assert.Equal(2L, (long)result.Rows[0]["num2"]);
    }

    [Fact]
    public async Task ReadAsync_ShouldMapDataCorrectly() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var sql = "SELECT 1 AS num1, 2 AS num2";
        var result = await cmd.ReadAsync(sql, rd => {
            var dt = new DataTable();
            dt.Load(rd);
            return dt;
        });

        Assert.Equal(2, result.Columns.Count);
        Assert.Single(result.Rows);
        Assert.Equal(1L, (long)result.Rows[0]["num1"]);
        Assert.Equal(2L, (long)result.Rows[0]["num2"]);
    }

    [Fact]
    public async Task StreamAsync_ShouldHandleEmptyResults() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var result = new List<TestClass>();
        var stream = cmd.StreamAsync(
            "SELECT description FROM test_values WHERE 1 = 0;",
            TestClassReader.Map);

        await foreach (var item in stream) {
            result.Add(item);
        }

        Assert.Empty(result);
    }

    [Fact]
    public void Execute_ShouldThrowExceptionForInvalidSql() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        Assert.Throws<DatabaseExecutionException>(() => {
            cmd.Execute("INVALID SQL;");
        });
    }

    [Fact]
    public async Task ExecuteAsync_ShouldThrowForNullCommandText() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var exception = await Assert.ThrowsAsync<DatabaseExecutionException>(async () => {
            await cmd.ExecuteAsync(null!);
        });

        Assert.Equal(DatabaseErrorCode.NoCommandText, exception.ErrorCode);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldThrowForInvalidCommandType() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var exception = await Assert.ThrowsAsync<DatabaseExecutionException>(async () => {
            await cmd.ExecuteAsync("INVALID SQL;", commandType: (CommandType)999);
        });

        Assert.Equal(DatabaseErrorCode.InvalidCommandType, exception.ErrorCode);
    }

    [Fact]
    public async Task ScalarAsync_ShouldReturnNullForEmptyResult() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var result = await cmd.ScalarAsync("SELECT NULL AS value;");
        Assert.Null(result);
    }

    [Fact]
    public async Task ScalarAsync_ShouldThrowForInvalidSql() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        await Assert.ThrowsAsync<DatabaseExecutionException>(async () => {
            await cmd.ScalarAsync("INVALID SQL;");
        });
    }

    [Fact]
    public async Task QueryAsync_ShouldReturnEmptyForNoResults() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var result = await cmd.QueryAsync(
            "SELECT * FROM test_values WHERE 1 = 0;",
            TestClassReader.Map);

        Assert.Empty(result);
    }

    [Fact]
    public async Task QuerySingleAsync_ShouldThrowForNoResults() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var result = await cmd.QuerySingleAsync(
            "SELECT 1 AS n WHERE 1 = 0;",
            rd => new { n = rd.GetInt32(0) });

        Assert.Null(result);
    }

    [Fact]
    public async Task ExecuteManyAsync_ShouldHandleEmptyParamList() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var paramList = Enumerable.Empty<DbParams>();

        await cmd.ExecuteManyAsync(
            "INSERT INTO test_values (description) VALUES (@description);",
            paramList);

        var result = await cmd.QueryAsync(
            "SELECT description FROM test_values WHERE description LIKE 'ExecuteManyAsync_ShouldHandleEmptyParamList%';",
            TestClassReader.Map);

        Assert.Empty(result);
    }

    [Fact]
    public async Task ReadAsync_ShouldThrowForInvalidMapping() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        var sql = "SELECT 1 AS num1, 2 AS num2";

        await Assert.ThrowsAsync<DatabaseExecutionException>(async () => {
            await cmd.ReadAsync<object>(sql, rd => {
                throw new InvalidOperationException("Simulated mapping error");
            });
        });
    }

    [Fact]
    public async Task ExecuteAsync_ShouldNotThrowForClosedConnection() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        conn.Close();

        await cmd.ExecuteAsync("INSERT INTO test_values (description) VALUES ('ExecuteAsync_ShouldNotThrowForClosedConnection');");

        var result = await cmd.ScalarAsync("SELECT description FROM test_values WHERE description = 'ExecuteAsync_ShouldNotThrowForClosedConnection';");

        Assert.Equal("ExecuteAsync_ShouldNotThrowForClosedConnection", result as string);
    }

    [Fact]
    public async Task ScalarAsync_ShouldNotThrowForClosedConnection() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        conn.Close();

        var result = await cmd.ScalarAsync("""
            SELECT 'ScalarAsync_ShouldNotThrowForClosedConnection' AS description;
            """);

        Assert.Equal("ScalarAsync_ShouldNotThrowForClosedConnection", result as string);
    }

    [Fact]
    public async Task QueryAsync_ShouldNotThrowForClosedConnection() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        conn.Close();

        var result = await cmd.QueryAsync(
            "SELECT 'QueryAsync_ShouldNotThrowForClosedConnection' AS description;",
            TestClassReader.Map);

        Assert.Single(result);
        Assert.Equal("QueryAsync_ShouldNotThrowForClosedConnection", result.First().Description);
    }

    [Fact]
    public async Task QuerySingleAsync_ShouldNotThrowForClosedConnection() {
        using var conn = testDb.CreateConnection();
        using var cmd = conn.CreateCommand();

        conn.Close();

        var result = await cmd.QuerySingleAsync(
            "SELECT 'QuerySingleAsync_ShouldNotThrowForClosedConnection' AS description;",
            TestClassReader.Map);

        Assert.Equal("QuerySingleAsync_ShouldNotThrowForClosedConnection", result.Description);
    }
}
