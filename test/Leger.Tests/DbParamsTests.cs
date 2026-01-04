namespace Leger.Tests;

using Xunit;

public class DbParamTests {
    [Fact]
    public void ShouldCreateEmptyParams() {
        var p = new DbParams();
        Assert.Empty(p.Keys);

        DbParams p2 = [];
        Assert.Empty(p2.Keys);
    }

    [Fact]
    public void ShouldCreateParamsWithOneEntry() {
        var p = new DbParams("key", 1);
        Assert.Equal(1, p["key"]);
    }

    [Fact]
    public void ShouldAddParams() {
        var p = new DbParams
        {
            { "key", 1 },
            { "key2", 2 }
        };
        Assert.Equal(1, p["key"]);
        Assert.Equal(2, p["key2"]);
    }

    [Fact]
    public void ShouldCombineParams() {
        var p1 = new DbParams("key", 1)
        {
            { "key2", 2 }
        };

        var p2 = new DbParams("key1", 3)
        {
            { "key2", "WRONG" }
        };

        p1.Add(p2);

        Assert.Equal(1, p1["key"]);
        Assert.Equal(2, p1["key2"]);
        Assert.Equal(3, p1["key1"]);
    }

    [Fact]
    public void ShouldHandleDuplicateKeysGracefully() {
        var p = new DbParams
        {
            { "key", 1 }
        };

        // Attempt to add a duplicate key
        p["key"] = 2;

        Assert.Equal(2, p["key"]);
    }

    [Fact]
    public void ShouldHandleNullValues() {
        var p = new DbParams
        {
            { "key", null! }
        };

        Assert.Null(p["key"]);
    }

    [Fact]
    public void ShouldAddEmptyParams() {
        var p1 = new DbParams
        {
            { "key", 1 }
        };

        var p2 = new DbParams();

        p1.Add(p2);

        Assert.Single(p1);
        Assert.Equal(1, p1["key"]);
    }

    [Fact]
    public void ShouldAddParamsWithNullValues() {
        var p1 = new DbParams
        {
            { "key", 1 }
        };

        var p2 = new DbParams
        {
            { "key2", null! }
        };

        p1.Add(p2);

        Assert.Equal(2, p1.Count);
        Assert.Equal(1, p1["key"]);
        Assert.Null(p1["key2"]);
    }

    [Fact]
    public void ShouldNotOverwriteExistingKeysWhenCombining() {
        var p1 = new DbParams
        {
            { "key", 1 }
        };

        var p2 = new DbParams
        {
            { "key", 2 }
        };

        p1.Add(p2);

        Assert.Equal(1, p1["key"]); // Original value should remain
    }

    [Fact]
    public void ShouldSupportMultipleDataTypes() {
        var p = new DbParams
        {
            { "int", 1 },
            { "string", "value" },
            { "bool", true },
            { "null", null! }
        };

        Assert.Equal(1, p["int"]);
        Assert.Equal("value", p["string"]);
        Assert.True((bool)p["bool"]);
        Assert.Null(p["null"]);
    }

    [Fact]
    public void ShouldThrowKeyNotFoundExceptionForMissingKey() {
        var p = new DbParams();

        Assert.Throws<KeyNotFoundException>(() => p["missing"]);
    }

    [Fact]
    public void ShouldAllowAddingDbTypeParamValues() {
        var dbTypeParam = new DbTypeParam(System.Data.DbType.String, "test");
        var p = new DbParams
        {
            { "key", dbTypeParam }
        };

        Assert.Equal(dbTypeParam, p["key"]);
    }
}
