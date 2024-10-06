namespace Leger.Tests;

using Xunit;

[Collection("TestDb")]
public class IDbConnectionTests(TestDb testDb)
{
    [Fact]
    public void CanExecute()
    {
        using var conn = testDb.CreateConnection();

        conn.Execute("""
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

        var result =
            conn.Query(
                "SELECT @description AS description",
                new("description", expected),
                rd => rd.ReadString("description"));

        Assert.Equal(expected, result.First());
    }

    [Fact]
    public void CanQuerySingle()
    {
        var expected = TestDb.GenerateRandomString();

        using var conn = testDb.CreateConnection();

        var result =
            conn.QuerySingle(
                "SELECT @description AS description",
                new("description", expected),
                rd => rd.ReadString("description"));

        Assert.Equal(expected, result);
    }

    [Fact]
    public void ReadShouldWork()
    {
        var expected = TestDb.GenerateRandomString();
        var sql = "SELECT 1 AS num1, 2 AS num2";
        using var conn = testDb.CreateConnection();
        var dt = conn.Read(sql, rd =>
        {
            var dt = new System.Data.DataTable();
            dt.Load(rd);
            return dt;
        });

        Assert.Equal(2, dt.Columns.Count);
        Assert.Single(dt.Rows);
    }
}
