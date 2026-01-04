namespace Leger.Tests;

using System.Data;
using Xunit;

[Collection("TestDb")]
public class IDbConnectionFactoryTests(TestDb testDb) {
    [Fact]
    public void CanExecute() {
        testDb.Execute("""
            INSERT INTO test_values (description) VALUES (@description);
            """,
            new("description", TestDb.GenerateRandomString()));

        Assert.True(true);
    }

    [Fact]
    public void CanScalar() {
        var expected = TestDb.GenerateRandomString();

        var result =
            testDb.Scalar(
                "SELECT @description AS description",
                new("description", expected));

        Assert.Equal(expected, result as string);
    }

    [Fact]
    public void CanQuery() {
        var expected = TestDb.GenerateRandomString();

        var result =
            testDb.Query(
                "SELECT @description AS description",
                new("description", expected),
                TestClassReader.Map);

        Assert.Equal(expected, result.First().Description);
    }

    [Fact]
    public void CanQuerySingle() {
        var expected = TestDb.GenerateRandomString();

        var result =
            testDb.QuerySingle(
                "SELECT @description AS description",
                new("description", expected),
                TestClassReader.Map);

        Assert.Equal(expected, result.Description);
    }

    [Fact]
    public void ReadShouldWork() {
        var expected = TestDb.GenerateRandomString();
        var sql = "SELECT 1 AS num1, 2 AS num2";
        var dt = testDb.Read(sql, rd => {
            var dt = new DataTable();
            dt.Load(rd);
            return dt;
        });

        Assert.Equal(2, dt.Columns.Count);
        Assert.Single(dt.Rows);
    }

    [Fact]
    public void Execute_ShouldThrowForNullCommandText() {
        var exception = Assert.Throws<DatabaseExecutionException>(() => {
            testDb.Execute(null!);
        });

        Assert.Equal(DatabaseErrorCode.NoCommandText, exception.ErrorCode);
    }

    [Fact]
    public void Execute_ShouldThrowForInvalidCommandType() {
        var exception = Assert.Throws<DatabaseExecutionException>(() => {
            testDb.Execute("INVALID SQL;", commandType: (CommandType)999);
        });

        Assert.Equal(DatabaseErrorCode.InvalidCommandType, exception.ErrorCode);
    }

    [Fact]
    public void Scalar_ShouldReturnNullForEmptyResult() {
        var result = testDb.Scalar("SELECT NULL AS value;");
        Assert.Null(result);
    }

    [Fact]
    public void Scalar_ShouldThrowForInvalidSql() {
        Assert.Throws<DatabaseExecutionException>(() => {
            testDb.Scalar("INVALID SQL;");
        });
    }

    [Fact]
    public void Query_ShouldReturnEmptyForNoResults() {
        var result = testDb.Query(
            "SELECT * FROM test_values WHERE 1 = 0;",
            TestClassReader.Map);

        Assert.Empty(result);
    }

    [Fact]
    public void QuerySingle_ShouldNotThrowForMultipleResults() {
        var result = testDb.QuerySingle(
            "SELECT 1 AS n UNION SELECT 2 AS n ORDER BY 1 ASC;",
            rd => rd.GetInt32(0));

        Assert.Equal(1, result);
    }

    [Fact]
    public void QuerySingle_ShouldNotThrowForNoResults() {
        var result = testDb.QuerySingle(
            "SELECT 1 AS n WHERE 1 = 0;",
            rd => new { n = rd.GetInt32(0) });

        Assert.Null(result);
    }

    [Fact]
    public void ExecuteMany_ShouldHandleEmptyParamList() {
        var paramList = Enumerable.Empty<DbParams>();

        testDb.ExecuteMany(
            "INSERT INTO test_values (description) VALUES (@description);",
            paramList);

        var result = testDb.Query(
            "SELECT description FROM test_values WHERE description LIKE 'ExecuteMany_ShouldHandleEmptyParamList%';",
            TestClassReader.Map);

        Assert.Empty(result);
    }

    [Fact]
    public void Read_ShouldThrowForInvalidMapping() {
        var sql = "SELECT 1 AS num1, 2 AS num2";

        Assert.Throws<DatabaseExecutionException>(() => {
            testDb.Read<object>(sql, rd => {
                throw new InvalidOperationException("Simulated mapping error");
            });
        });
    }
}

[Collection("TestDb")]
public class IDbConnectionFactoryTestsAsync(TestDb testDb) {
    [Fact]
    public async Task CanExecuteAsync() {
        await testDb.ExecuteAsync("""
            INSERT INTO test_values (description) VALUES (@description);
            """,
            new("description", TestDb.GenerateRandomString()));

        Assert.True(true);
    }

    [Fact]
    public async Task CanScalarAsync() {
        var expected = TestDb.GenerateRandomString();

        var result =
            await testDb.ScalarAsync(
                "SELECT @description AS description",
                new("description", expected));

        Assert.Equal(expected, result as string);
    }

    [Fact]
    public async Task CanQueryAsync() {
        var expected = TestDb.GenerateRandomString();

        var result =
            await testDb.QueryAsync(
                "SELECT @description AS description",
                new("description", expected),
                TestClassReader.Map);

        Assert.Equal(expected, result.First().Description);
    }

    [Fact]
    public async Task CanQuerySingleAsync() {
        var expected = TestDb.GenerateRandomString();

        var result =
            await testDb.QuerySingleAsync(
                "SELECT @description AS description",
                new("description", expected),
                TestClassReader.Map);

        Assert.Equal(expected, result.Description);
    }

    [Fact]
    public async Task ReadShouldWorkAsync() {
        var expected = TestDb.GenerateRandomString();
        var sql = "SELECT 1 AS num1, 2 AS num2";
        var dt = await testDb.ReadAsync(sql, rd => {
            var dt = new DataTable();
            dt.Load(rd);
            return dt;
        });

        Assert.Equal(2, dt.Columns.Count);
        Assert.Single(dt.Rows);
    }


    [Fact]
    public async Task CanStreamAsync() {
        var expected = TestDb.GenerateRandomString();

        var result = new List<TestClass>();
        var stream = testDb.StreamAsync(
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

        var result = new List<TestClass>();
        var stream = testDb.StreamAsync(
            $"SELECT '{expected}' AS description UNION SELECT 'a' UNION SELECT 'b'", // we only bypass sql params here for testing purposes
            TestClassReader.Map);

        await foreach (var item in stream) {
            result.Add(item);
        }

        Assert.Equal(3, result.Count);
        Assert.Contains(result, x => x.Description == expected);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldThrowForNullCommandText() {
        var exception = await Assert.ThrowsAsync<DatabaseExecutionException>(async () => {
            await testDb.ExecuteAsync(null!);
        });

        Assert.Equal(DatabaseErrorCode.NoCommandText, exception.ErrorCode);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldThrowForInvalidCommandType() {
        var exception = await Assert.ThrowsAsync<DatabaseExecutionException>(async () => {
            await testDb.ExecuteAsync("INVALID SQL;", commandType: (CommandType)999);
        });

        Assert.Equal(DatabaseErrorCode.InvalidCommandType, exception.ErrorCode);
    }

    [Fact]
    public async Task ScalarAsync_ShouldReturnNullForEmptyResult() {
        var result = await testDb.ScalarAsync("SELECT NULL AS value;");
        Assert.Null(result);
    }

    [Fact]
    public async Task StreamAsync_ShouldHandleEmptyResults() {
        var result = new List<TestClass>();
        var stream = testDb.StreamAsync(
            "SELECT description FROM test_values WHERE 1 = 0;",
            TestClassReader.Map);

        await foreach (var item in stream) {
            result.Add(item);
        }

        Assert.Empty(result);
    }

    [Fact]
    public async Task ScalarAsync_ShouldThrowForInvalidSql() {
        await Assert.ThrowsAsync<DatabaseExecutionException>(async () => {
            await testDb.ScalarAsync("INVALID SQL;");
        });
    }

    [Fact]
    public async Task QueryAsync_ShouldReturnEmptyForNoResults() {
        var result = await testDb.QueryAsync(
            "SELECT * FROM test_values WHERE 1 = 0;",
            TestClassReader.Map);

        Assert.Empty(result);
    }

    [Fact]
    public async Task QuerySingleAsync_ShouldNotThrowForMultipleResults() {
        var result = await testDb.QuerySingleAsync(
            "SELECT 1 AS n UNION SELECT 2 AS n ORDER BY 1 ASC;",
            rd => rd.GetInt32(0));

        Assert.Equal(1, result);
    }

    [Fact]
    public async Task QuerySingleAsync_ShouldNotThrowForNoResults() {
        var result = await testDb.QuerySingleAsync(
            "SELECT 1 AS n WHERE 1 = 0;",
            rd => new { n = rd.GetInt32(0) });

        Assert.Null(result);
    }

    [Fact]
    public async Task ExecuteManyAsync_ShouldHandleEmptyParamList() {
        var paramList = Enumerable.Empty<DbParams>();

        await testDb.ExecuteManyAsync(
            "INSERT INTO test_values (description) VALUES (@description);",
            paramList);

        var result = await testDb.QueryAsync(
            "SELECT description FROM test_values WHERE description LIKE 'ExecuteMany_ShouldHandleEmptyParamList%';",
            TestClassReader.Map);

        Assert.Empty(result);
    }

    [Fact]
    public async Task ReadAsync_ShouldThrowForInvalidMapping() {
        var sql = "SELECT 1 AS num1, 2 AS num2";

        await Assert.ThrowsAsync<DatabaseExecutionException>(async () => {
            await testDb.ReadAsync<object>(sql, rd => {
                throw new InvalidOperationException("Simulated mapping error");
            });
        });
    }

    [Fact]
    public async Task ReadShouldWorkAsyncWithAsyncMap() {
        var expected = TestDb.GenerateRandomString();
        var sql = "SELECT 1 AS num UNION SELECT 2";

        var result = await testDb.ReadAsync(sql, rd => {
            return rd.MapAsync(r => r.GetInt32(0));
        });

        Assert.Equal(2, result.Count());
    }
}
