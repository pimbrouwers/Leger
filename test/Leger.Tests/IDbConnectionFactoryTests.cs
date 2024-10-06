namespace Leger.Tests;

using Xunit;

[Collection("TestDb")]
public class IDbConnectionFactoryTests(TestDb testDb)
{
    [Fact]
    public void CanExecute()
    {
        testDb.Execute("""
            INSERT INTO test_values (description) VALUES (@description);
            """,
            new("description", TestDb.GenerateRandomString()));

        Assert.True(true);
    }

    [Fact]
    public void CanScalar()
    {
        var expected = TestDb.GenerateRandomString();

        var result =
            testDb.Scalar(
                "SELECT @description AS description",
                new("description", expected));

        Assert.Equal(expected, result as string);
    }

    [Fact]
    public void CanQuery()
    {
        var expected = TestDb.GenerateRandomString();

        var result =
            testDb.Query(
                "SELECT @description AS description",
                new("description", expected),
                rd => rd.ReadString("description"));

        Assert.Equal(expected, result.First());
    }

    [Fact]
    public void CanQuerySingle()
    {
        var expected = TestDb.GenerateRandomString();

        var result =
            testDb.QuerySingle(
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
        var dt = testDb.Read(sql, rd =>
        {
            var dt = new System.Data.DataTable();
            dt.Load(rd);
            return dt;
        });

        Assert.Equal(2, dt.Columns.Count);
        Assert.Single(dt.Rows);
    }
}
