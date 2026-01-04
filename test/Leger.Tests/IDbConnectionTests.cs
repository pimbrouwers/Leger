namespace Leger.Tests;

using System.Data;
using Xunit;

[Collection("TestDb")]
public class IDbConnectionTests(TestDb testDb) {
    [Fact]
    public void CanExecute() {
        using var conn = testDb.CreateConnection();

        conn.Execute("""
            INSERT INTO test_values (description) VALUES (@description);
            """,
            new("description", TestDb.GenerateRandomString()));

        Assert.True(true);
    }

    [Fact]
    public void CanScalar() {
        var expected = TestDb.GenerateRandomString();

        using var conn = testDb.CreateConnection();

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

        var result =
            conn.Query(
                "SELECT @description AS description",
                new("description", expected),
                TestClassReader.Map);

        Assert.Equal(expected, result.First().Description);
    }

    [Fact]
    public void CanQuerySingle() {
        var expected = TestDb.GenerateRandomString();

        using var conn = testDb.CreateConnection();

        var result =
            conn.QuerySingle(
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
        var dt = conn.Read(sql, rd => {
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

        using var conn = testDb.CreateConnection();

        var result = new List<TestClass>();
        var stream = conn.StreamAsync(
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

        var result = new List<TestClass>();
        var stream = conn.StreamAsync(
            $"SELECT '{expected}' AS description UNION SELECT 'a' UNION SELECT 'b'", // we only bypass sql params here for testing purposes
            TestClassReader.Map);

        await foreach (var item in stream) {
            result.Add(item);
        }

        Assert.Equal(3, result.Count);
        Assert.Contains(result, x => x.Description == expected);
    }

    [Fact]
    public void Execute_ShouldThrowForNullCommandText() {
        using var conn = testDb.CreateConnection();

        var exception = Assert.Throws<DatabaseExecutionException>(() => {
            conn.Execute(null!);
        });

        Assert.Equal(DatabaseErrorCode.NoCommandText, exception.ErrorCode);
    }

    [Fact]
    public void Execute_ShouldThrowForInvalidCommandType() {
        using var conn = testDb.CreateConnection();

        var exception = Assert.Throws<DatabaseExecutionException>(() => {
            conn.Execute("INVALID SQL;", commandType: (CommandType)999);
        });

        Assert.Equal(DatabaseErrorCode.InvalidCommandType, exception.ErrorCode);
    }

    [Fact]
    public void Scalar_ShouldReturnNullForEmptyResult() {
        using var conn = testDb.CreateConnection();

        var result = conn.Scalar("SELECT NULL AS value;");
        Assert.Null(result);
    }

    [Fact]
    public async Task StreamAsync_ShouldHandleEmptyResults() {
        using var conn = testDb.CreateConnection();

        var result = new List<TestClass>();
        var stream = conn.StreamAsync(
            "SELECT description FROM test_values WHERE 1 = 0;",
            TestClassReader.Map);

        await foreach (var item in stream) {
            result.Add(item);
        }

        Assert.Empty(result);
    }

    [Fact]
    public void Scalar_ShouldThrowForInvalidSql() {
        using var conn = testDb.CreateConnection();

        Assert.Throws<DatabaseExecutionException>(() => {
            conn.Scalar("INVALID SQL;");
        });
    }

    [Fact]
    public void Query_ShouldReturnEmptyForNoResults() {
        using var conn = testDb.CreateConnection();

        var result = conn.Query(
            "SELECT * FROM test_values WHERE 1 = 0;",
            TestClassReader.Map);

        Assert.Empty(result);
    }

    [Fact]
    public void QuerySingle_ShouldNotThrowForMultipleResults() {
        using var conn = testDb.CreateConnection();

        var result = conn.QuerySingle(
            "SELECT 1 AS n UNION SELECT 2 AS n ORDER BY 1 ASC;",
            rd => rd.GetInt32(0));

        Assert.Equal(1, result);
    }

    [Fact]
    public void QuerySingle_ShouldNotThrowForNoResults() {
        using var conn = testDb.CreateConnection();

        var result = conn.QuerySingle(
            "SELECT 1 AS n WHERE 1 = 0;",
            rd => new { n = rd.GetInt32(0) });

        Assert.Null(result);
    }

    [Fact]
    public void ExecuteMany_ShouldHandleEmptyParamList() {
        using var conn = testDb.CreateConnection();

        var paramList = Enumerable.Empty<DbParams>();

        conn.ExecuteMany(
            "INSERT INTO test_values (description) VALUES (@description);",
            paramList);

        var result = conn.Query(
            "SELECT description FROM test_values WHERE description LIKE 'ExecuteMany_ShouldHandleEmptyParamList%';",
            TestClassReader.Map);

        Assert.Empty(result);
    }

    [Fact]
    public void Read_ShouldThrowForInvalidMapping() {
        using var conn = testDb.CreateConnection();

        var sql = "SELECT 1 AS num1, 2 AS num2";

        Assert.Throws<DatabaseExecutionException>(() => {
            conn.Read<object>(sql, rd => {
                throw new InvalidOperationException("Simulated mapping error");
            });
        });
    }

    [Fact]
    public void CreateTransaction_ShouldOpenConnectionIfClosed() {
        using var conn = testDb.CreateConnection();

        conn.Close();
        using var transaction = conn.CreateTransaction();

        Assert.Equal(ConnectionState.Open, conn.State);
        Assert.NotNull(transaction);
    }

    [Fact]
    public void CreateTransaction_ShouldThrowIfConnectionCannotBeOpened() {
        // we use a custom connection that simulates failure to open
        using var conn = new UnrecoverableConnection();

        conn.Close();
        conn.Dispose();

        Assert.Throws<DatabaseConnectionException>(() => {
            conn.CreateTransaction();
        });
    }

    private sealed class UnrecoverableConnection : IDbConnection {
        private bool _isDisposed;

        public void Dispose() {
            _isDisposed = true;
        }

        public void Open() {
            if (_isDisposed) {
                throw new ObjectDisposedException("Connection");
            }
        }

#pragma warning disable CS8767
        public string ConnectionString { get; set; } = "";
#pragma warning restore CS8767
        public int ConnectionTimeout => 30;
        public string Database => "TestDb";
        public ConnectionState State => _isDisposed ? ConnectionState.Closed : ConnectionState.Open;
        public IDbTransaction BeginTransaction() => throw new NotImplementedException();
        public IDbTransaction BeginTransaction(IsolationLevel il) => throw new NotImplementedException();
        public void ChangeDatabase(string databaseName) => throw new NotImplementedException();
        public IDbCommand CreateCommand() => throw new NotImplementedException();
        public void Close() => _isDisposed = true;
    }
}


[Collection("TestDb")]
public class IDbConnectionTestsAsync(TestDb testDb) {
    [Fact]
    public async Task CanExecuteAsync() {
        using var conn = testDb.CreateConnection();

        await conn.ExecuteAsync("""
            INSERT INTO test_values (description) VALUES (@description);
            """,
            new("description", TestDb.GenerateRandomString()));

        Assert.True(true);
    }

    [Fact]
    public async Task CanScalarAsync() {
        var expected = TestDb.GenerateRandomString();

        using var conn = testDb.CreateConnection();

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

        var result =
            await conn.QueryAsync(
                "SELECT @description AS description",
                new("description", expected),
                TestClassReader.Map);

        Assert.Equal(expected, result.First().Description);
    }

    [Fact]
    public async Task CanQuerySingleAsync() {
        var expected = TestDb.GenerateRandomString();

        using var conn = testDb.CreateConnection();

        var result =
            await conn.QuerySingleAsync(
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
        var dt = await conn.ReadAsync(sql, rd => {
            var dt = new DataTable();
            dt.Load(rd);
            return dt;
        });

        Assert.Equal(2, dt.Columns.Count);
        Assert.Single(dt.Rows);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldThrowForNullCommandText() {
        using var conn = testDb.CreateConnection();

        var exception = await Assert.ThrowsAsync<DatabaseExecutionException>(async () => {
            await conn.ExecuteAsync(null!);
        });

        Assert.Equal(DatabaseErrorCode.NoCommandText, exception.ErrorCode);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldThrowForInvalidCommandType() {
        using var conn = testDb.CreateConnection();

        var exception = await Assert.ThrowsAsync<DatabaseExecutionException>(async () => {
            await conn.ExecuteAsync("INVALID SQL;", commandType: (CommandType)999);
        });

        Assert.Equal(DatabaseErrorCode.InvalidCommandType, exception.ErrorCode);
    }

    [Fact]
    public async Task ScalarAsync_ShouldReturnNullForEmptyResult() {
        using var conn = testDb.CreateConnection();

        var result = await conn.ScalarAsync("SELECT NULL AS value;");
        Assert.Null(result);
    }

    [Fact]
    public async Task ScalarAsync_ShouldThrowForInvalidSql() {
        using var conn = testDb.CreateConnection();

        await Assert.ThrowsAsync<DatabaseExecutionException>(async () => {
            await conn.ScalarAsync("INVALID SQL;");
        });
    }

    [Fact]
    public async Task QueryAsync_ShouldReturnEmptyForNoResults() {
        using var conn = testDb.CreateConnection();

        var result = await conn.QueryAsync(
            "SELECT * FROM test_values WHERE 1 = 0;",
            TestClassReader.Map);

        Assert.Empty(result);
    }

    [Fact]
    public async Task QuerySingleAsync_ShouldNotThrowForMultipleResults() {
        using var conn = testDb.CreateConnection();

        var result = await conn.QuerySingleAsync(
            "SELECT 1 AS n UNION SELECT 2 AS n ORDER BY 1 ASC;",
            rd => rd.GetInt32(0));

        Assert.Equal(1, result);
    }

    [Fact]
    public async Task QuerySingleAsync_ShouldNotThrowForNoResults() {
        using var conn = testDb.CreateConnection();

        var result = await conn.QuerySingleAsync(
            "SELECT 1 AS n WHERE 1 = 0;",
            rd => new { n = rd.GetInt32(0) });

        Assert.Null(result);
    }

    [Fact]
    public async Task ExecuteManyAsync_ShouldHandleEmptyParamList() {
        using var conn = testDb.CreateConnection();

        var paramList = Enumerable.Empty<DbParams>();

        await conn.ExecuteManyAsync(
            "INSERT INTO test_values (description) VALUES (@description);",
            paramList);

        var result = await conn.QueryAsync(
            "SELECT description FROM test_values WHERE description LIKE 'ExecuteMany_ShouldHandleEmptyParamList%';",
            TestClassReader.Map);

        Assert.Empty(result);
    }

    [Fact]
    public async Task ReadAsync_ShouldThrowForInvalidMapping() {
        using var conn = testDb.CreateConnection();

        var sql = "SELECT 1 AS num1, 2 AS num2";

        await Assert.ThrowsAsync<DatabaseExecutionException>(async () => {
            await conn.ReadAsync<object>(sql, rd => {
                throw new InvalidOperationException("Simulated mapping error");
            });
        });
    }

    [Fact]
    public async Task ReadShouldWorkAsyncWithAsyncMap() {
        var expected = TestDb.GenerateRandomString();
        var sql = "SELECT 1 AS num UNION SELECT 2";
        using var conn = testDb.CreateConnection();

        var result = await conn.ReadAsync(sql, rd => {
            return rd.MapAsync(r => r.GetInt32(0));
        });

        Assert.Equal(2, result.Count());
    }
}
