namespace Leger.Tests;

using Xunit;

[Collection("TestDb")]
public class IDataReaderTests(TestDb testDb) {
    [Fact]
    public void Map_ShouldReturnMappedResults() {
        var expected = TestDb.GenerateRandomString();
        using var conn = testDb.CreateConnection();
        var result = conn.Read(
            "SELECT @description AS description",
            new("description", expected),
            rd => rd.Map(TestClassReader.Map).ToList());
        Assert.Single(result);
        Assert.Equal(expected, result.First().Description);
    }

    [Fact]
    public void MapFirst_ShouldReturnFirstResult() {
        var expected = TestDb.GenerateRandomString();
        using var conn = testDb.CreateConnection();
        var result = conn.Read(
            "SELECT @description AS description, 1 AS n UNION SELECT 'Other', 2 ORDER BY n ASC;",
            new("description", expected),
            rd => rd.MapFirst(TestClassReader.Map));
        Assert.Equal(expected, result.Description);
    }

    [Fact]
    public void MapFirst_ShouldReturnDefaultForNoResults() {
        using var conn = testDb.CreateConnection();
        var result = conn.Read(
            "SELECT * FROM test_values WHERE 1 = 0;",
            rd => rd.MapFirst(TestClassReader.Map));
        Assert.Null(result);
    }

    [Fact]
    public void MapNext_ShouldReturnResultsFromNextResultSet() {
        using var conn = testDb.CreateConnection();
        var result = conn.Read(
            "SELECT 1 AS id; SELECT 2 AS id;",
            rd => rd.MapNext(r => r.GetInt32(0)).ToList());
        Assert.Single(result);
        Assert.Equal(2, result.First());
    }

    [Fact]
    public void MapFirstNext_ShouldReturnFirstResultFromNextResultSet() {
        using var conn = testDb.CreateConnection();
        var result = conn.Read(
            "SELECT 1 AS id; SELECT 2 AS id;",
            rd => rd.MapFirstNext(r => r.GetInt32(0)));
        Assert.Equal(2, result);
    }

    [Fact]
    public async Task MapAsync_ShouldReturnMappedResults() {
        var expected = TestDb.GenerateRandomString();
        using var conn = testDb.CreateConnection();
        var result = await conn.ReadAsync(
            "SELECT @description AS description",
            new("description", expected),
            rd => rd.MapAsync(TestClassReader.Map));
        Assert.Single(result);
        Assert.Equal(expected, result.First().Description);
    }

    [Fact]
    public async Task MapFirstAsync_ShouldReturnFirstResult() {
        var expected = TestDb.GenerateRandomString();
        using var conn = testDb.CreateConnection();
        var result = await conn.ReadAsync(
            "SELECT @description AS description, 1 AS n UNION SELECT 'Other', 2 ORDER BY n ASC;",
            new("description", expected),
            async rd => await rd.MapFirstAsync(TestClassReader.Map));
        Assert.Equal(expected, result.Description);
    }

    [Fact]
    public async Task MapFirstAsync_ShouldReturnDefaultForNoResults() {
        var result = await testDb.ReadAsync(
            "SELECT * FROM test_values WHERE 1 = 0;",
            rd => rd.MapFirstAsync(TestClassReader.Map));
        Assert.Null(result);
    }

    [Fact]
    public async Task MapNextAsync_ShouldReturnResultsFromNextResultSet() {
        using var conn = testDb.CreateConnection();
        var result = await conn.ReadAsync(
            "SELECT 1 AS id; SELECT 2 AS id;",
            async rd => await rd.MapNextAsync(r => r.GetInt32(0)));
        Assert.Single(result);
        Assert.Equal(2, result.First());
    }

    [Fact]
    public async Task MapFirstNextAsync_ShouldReturnFirstResultFromNextResultSet() {
        using var conn = testDb.CreateConnection();
        var result = await conn.ReadAsync(
            "SELECT 1 AS id; SELECT 2 AS id;",
            async rd => await rd.MapFirstNextAsync(r => r.GetInt32(0)));
        Assert.Equal(2, result);
    }

    // [Fact]
    // public async Task MapStreamAsync_ShouldStreamResults() {
    //     var expected = TestDb.GenerateRandomString();
    //     using var conn = testDb.CreateConnection();
    //     var result = new List<TestClass>();
    //     await foreach (var item in conn.ReadAsync(
    //         "SELECT @description AS description",
    //         new("description", expected),
    //         rd => rd.MapStreamAsync(TestClassReader.Map))) {
    //         result.Add(item);
    //     }
    //     Assert.Single(result);
    //     Assert.Equal(expected, result.First().Description);
    // }

    // [Fact]
    // public async Task MapNextStreamAsync_ShouldStreamResultsFromNextResultSet() {
    //     using var conn = testDb.CreateConnection();
    //     var result = new List<int>();
    //     await foreach (var item in conn.ReadAsync(
    //         "SELECT 1 AS id; SELECT 2 AS id;",
    //         rd => rd.MapNextStreamAsync(r => r.GetInt32(0)))) {
    //         result.Add(item);
    //     }
    //     Assert.Single(result);
    //     Assert.Equal(2, result.First());
    // }
}
