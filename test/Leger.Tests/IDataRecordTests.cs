namespace Leger.Tests;

using System.Data;
using System.Security.Cryptography;
using System.Text;
using Xunit;

[Collection("TestDb")]
public class IDataRecordTests(TestDb testDb) {
    [Fact]
    public void SelectAllTypes() {
        var sql = @"
            SELECT  @p_GetString AS p_GetString
                  , @p_GetChar AS p_GetChar
                  , @p_GetBoolean AS p_GetBoolean
                  , @p_GetByte AS p_GetByte
                  , @p_GetInt16 AS p_GetInt16
                  , @p_GetInt32 AS p_GetInt32
                  , @p_GetInt64 AS p_GetInt64
                  , @p_GetDecimal AS p_GetDecimal
                  , @p_GetDouble AS p_GetDouble
                  , @p_GetFloat AS p_GetFloat
                  , @p_GetGuid AS p_GetGuid
                  , @p_GetDateTime AS p_GetDateTime
                  , @p_GetDateOnly AS p_GetDateOnly
                  , @p_GetBytes AS p_GetBytes
            ";

        var now = DateTime.Now;
        var nowDateOnly = DateOnly.FromDateTime(now);

        var param = new DbParams(){
                { "p_GetString", "Nhlpa.Sql" },
                { "p_GetChar", 's' },
                { "p_GetBoolean", DbTypeParam.Boolean(true) },
                { "p_GetByte", DbTypeParam.Byte(1) },
                { "p_GetInt16", DbTypeParam.Int16(1) },
                { "p_GetInt32", DbTypeParam.Int32(1) },
                { "p_GetInt64", DbTypeParam.Int64(1L) },
                { "p_GetDecimal", DbTypeParam.Decimal(1.0M) },
                { "p_GetDouble", DbTypeParam.Double(1.0) },
                { "p_GetFloat", DbTypeParam.Float(1.0F) },
                { "p_GetGuid", DbTypeParam.Guid(Guid.Empty) },
                { "p_GetDateTime", now },
                { "p_GetDateOnly", nowDateOnly },
                { "p_GetBytes", Array.Empty<byte>() }
            };

        using var conn = testDb.CreateConnection();
        var result = conn.QuerySingle(sql, param, rd => {
            Assert.Equal("Nhlpa.Sql", rd.ReadString("p_GetString"));
            Assert.Equal('s', rd.ReadChar("p_GetChar"));
            Assert.True(rd.ReadBoolean("p_GetBoolean"));
            Assert.Equal(1, rd.ReadByte("p_GetByte"));
            Assert.Equal(1, rd.ReadInt16("p_GetInt16"));
            Assert.Equal(1, rd.ReadInt32("p_GetInt32"));
            Assert.Equal(1L, rd.ReadInt64("p_GetInt64"));
            Assert.Equal(1.0M, rd.ReadDecimal("p_GetDecimal"));
            Assert.Equal(1.0, rd.ReadDouble("p_GetDouble"));
            Assert.Equal(1.0, rd.ReadFloat("p_GetFloat"));
            Assert.Equal(Guid.Empty, rd.ReadGuid("p_GetGuid"));
            Assert.Equal(now, rd.ReadDateTime("p_GetDateTime"));
            Assert.Equal(nowDateOnly, rd.ReadDateOnly("p_GetDateOnly"));

            Assert.Equal("Nhlpa.Sql", rd.ReadNullableString("p_GetString"));
            Assert.Equal('s', rd.ReadNullableChar("p_GetChar"));
            Assert.True(rd.ReadNullableBoolean("p_GetBoolean"));
            Assert.Equal((byte)1, rd.ReadNullableByte("p_GetByte"));
            Assert.Equal((short)1, rd.ReadNullableInt16("p_GetInt16"));
            Assert.Equal(1, rd.ReadNullableInt32("p_GetInt32"));
            Assert.Equal(1L, rd.ReadNullableInt64("p_GetInt64"));
            Assert.Equal(1.0M, rd.ReadNullableDecimal("p_GetDecimal"));
            Assert.Equal(1.0, rd.ReadNullableDouble("p_GetDouble"));
            Assert.Equal((float)1.0, rd.ReadNullableFloat("p_GetFloat"));
            Assert.Equal(Guid.Empty, rd.ReadNullableGuid("p_GetGuid"));
            Assert.Equal(now, rd.ReadNullableDateTime("p_GetDateTime"));
            Assert.Equal(nowDateOnly, rd.ReadNullableDateOnly("p_GetDateOnly"));

            return 1;
        });

        Assert.Equal(1, result);
    }

    [Fact]
    public void SelectAllTypes_Nullable_NullValue() {
        var sql = @"
            SELECT  NULL AS p_GetNullableBoolean
                  , NULL AS p_GetNullableByte
                  , NULL AS p_GetNullableInt16
                  , NULL AS p_GetNullableInt32
                  , NULL AS p_GetNullableInt64
                  , NULL AS p_GetNullableDecimal
                  , NULL AS p_GetNullableDouble
                  , NULL AS p_GetNullableFloat
                  , NULL AS p_GetNullableGuid
                  , NULL AS p_GetNullableDateTime
                  , NULL AS p_GetNullableDateOnly
                  , NULL AS p_GetNullableBytes
            ";

        var now = DateTime.Now;
        var nowDateOnly = DateOnly.FromDateTime(now);


        using var conn = testDb.CreateConnection();
        var result = conn.QuerySingle(sql, rd => {
            Assert.Null(rd.ReadNullableBoolean("p_GetNullableBoolean"));
            Assert.Null(rd.ReadNullableByte("p_GetNullableByte"));
            Assert.Null(rd.ReadNullableInt16("p_GetNullableInt16"));
            Assert.Null(rd.ReadNullableInt32("p_GetNullableInt32"));
            Assert.Null(rd.ReadNullableInt64("p_GetNullableInt64"));
            Assert.Null(rd.ReadNullableDecimal("p_GetNullableDecimal"));
            Assert.Null(rd.ReadNullableDouble("p_GetNullableDouble"));
            Assert.Null(rd.ReadNullableFloat("p_GetNullableFloat"));
            Assert.Null(rd.ReadNullableGuid("p_GetNullableGuid"));
            Assert.Null(rd.ReadNullableDateTime("p_GetNullableDateTime"));
            Assert.Null(rd.ReadNullableDateOnly("p_GetNullableDateOnly"));

            return 1;
        });

        Assert.Equal(1, result);
    }

    [Fact]
    public void SelectAllTypesByIndex() {
        var sql = @"
            SELECT  @p_GetString AS p_GetString
                  , @p_GetChar AS p_GetChar
                  , @p_GetBoolean AS p_GetBoolean
                  , @p_GetByte AS p_GetByte
                  , @p_GetInt16 AS p_GetInt16
                  , @p_GetInt32 AS p_GetInt32
                  , @p_GetInt64 AS p_GetInt64
                  , @p_GetDecimal AS p_GetDecimal
                  , @p_GetDouble AS p_GetDouble
                  , @p_GetFloat AS p_GetFloat
                  , @p_GetGuid AS p_GetGuid
                  , @p_GetDateTime AS p_GetDateTime
                  , @p_GetDateOnly AS p_GetDateOnly
                  , @p_GetBytes AS p_GetBytes
            ";

        var now = DateTime.Now;
        var nowDateOnly = DateOnly.FromDateTime(now);

        var param = new DbParams(){
                { "p_GetString", "Nhlpa.Sql" },
                { "p_GetChar", 's' },
                { "p_GetBoolean", DbTypeParam.Boolean(true) },
                { "p_GetByte", DbTypeParam.Byte(1) },
                { "p_GetInt16", DbTypeParam.Int16(1) },
                { "p_GetInt32", DbTypeParam.Int32(1) },
                { "p_GetInt64", DbTypeParam.Int64(1L) },
                { "p_GetDecimal", DbTypeParam.Decimal(1.0M) },
                { "p_GetDouble", DbTypeParam.Double(1.0) },
                { "p_GetFloat", DbTypeParam.Float(1.0F) },
                { "p_GetGuid", DbTypeParam.Guid(Guid.Empty) },
                { "p_GetDateTime", now },
                { "p_GetDateOnly", nowDateOnly },
                { "p_GetBytes", Array.Empty<byte>() }
            };

        using var conn = testDb.CreateConnection();
        var result = conn.QuerySingle(sql, param, rd => {
            Assert.Equal("Nhlpa.Sql", rd.ReadString(0));
            Assert.Equal('s', rd.ReadChar(1));
            Assert.True(rd.ReadBoolean(2));
            Assert.Equal(1, rd.ReadByte(3));
            Assert.Equal(1, rd.ReadInt16(4));
            Assert.Equal(1, rd.ReadInt32(5));
            Assert.Equal(1L, rd.ReadInt64(6));
            Assert.Equal(1.0M, rd.ReadDecimal(7));
            Assert.Equal(1.0, rd.ReadDouble(8));
            Assert.Equal(1.0, rd.ReadFloat(9));
            Assert.Equal(Guid.Empty, rd.ReadGuid(10));
            Assert.Equal(now, rd.ReadDateTime(11));
            Assert.Equal(nowDateOnly, rd.ReadDateOnly(12));

            Assert.Equal("Nhlpa.Sql", rd.ReadNullableString(0));
            Assert.Equal('s', rd.ReadNullableChar(1));
            Assert.True(rd.ReadNullableBoolean(2));
            Assert.Equal((byte)1, rd.ReadNullableByte(3));
            Assert.Equal((short)1, rd.ReadNullableInt16(4));
            Assert.Equal(1, rd.ReadNullableInt32(5));
            Assert.Equal(1L, rd.ReadNullableInt64(6));
            Assert.Equal(1.0M, rd.ReadNullableDecimal(7));
            Assert.Equal(1.0, rd.ReadNullableDouble(8));
            Assert.Equal((float)1.0, rd.ReadNullableFloat(9));
            Assert.Equal(Guid.Empty, rd.ReadNullableGuid(10));
            Assert.Equal(now, rd.ReadNullableDateTime(11));
            Assert.Equal(nowDateOnly, rd.ReadNullableDateOnly(12));

            return 1;
        });

        Assert.Equal(1, result);
    }

    [Fact]
    public void SelectAllTypesByIndex_Nullable_NullValue() {
        var sql = @"
            SELECT  NULL AS p_GetNullableBoolean
                  , NULL AS p_GetNullableByte
                  , NULL AS p_GetNullableInt16
                  , NULL AS p_GetNullableInt32
                  , NULL AS p_GetNullableInt64
                  , NULL AS p_GetNullableDecimal
                  , NULL AS p_GetNullableDouble
                  , NULL AS p_GetNullableFloat
                  , NULL AS p_GetNullableGuid
                  , NULL AS p_GetNullableDateTime
                  , NULL AS p_GetNullableDateOnly
                  , NULL AS p_GetNullableBytes
            ";

        var now = DateTime.Now;
        var nowDateOnly = DateOnly.FromDateTime(now);


        using var conn = testDb.CreateConnection();
        var result = conn.QuerySingle(sql, rd => {
            Assert.Null(rd.ReadNullableBoolean(0));
            Assert.Null(rd.ReadNullableByte(1));
            Assert.Null(rd.ReadNullableInt16(2));
            Assert.Null(rd.ReadNullableInt32(3));
            Assert.Null(rd.ReadNullableInt64(4));
            Assert.Null(rd.ReadNullableDecimal(5));
            Assert.Null(rd.ReadNullableDouble(6));
            Assert.Null(rd.ReadNullableFloat(7));
            Assert.Null(rd.ReadNullableGuid(8));
            Assert.Null(rd.ReadNullableDateTime(9));
            Assert.Null(rd.ReadNullableDateOnly(10));

            return 1;
        });

        Assert.Equal(1, result);
    }

    [Fact]
    public void ReadBytesShouldWork() {
        var testString = "A sample of bytes";
        var bytes = Encoding.UTF8.GetBytes(testString);

        var result =
            testDb.QuerySingle("""
                INSERT INTO file (data) VALUES (@data);
                SELECT data FROM file WHERE file_id = LAST_INSERT_ROWID();
                """,
                new("data", bytes),
                rd => rd.ReadBytes("data"));

        var resultStr = Encoding.UTF8.GetString(result ?? []);
        Assert.Equal(resultStr, testString);
    }

    [Fact]
    public void ReadNonExistentField_ShouldThrowFieldNotFoundException() {
        var sql = "SELECT 1 AS existing_field";

        using var conn = testDb.CreateConnection();
        var exception = Assert.Throws<DatabaseReadFieldException>(() => {
            conn.QuerySingle(sql, rd => rd.ReadInt32("non_existent_field"));
        });

        Assert.Equal(DatabaseErrorCode.FieldNotFound, exception.ErrorCode);
        Assert.Contains("non_existent_field", exception.Message);
    }

    [Fact]
    public void ReadBytes_ShouldHandleLargeByteArrays() {
        var largeData = RandomNumberGenerator.GetBytes(10_000);

        var result = testDb.QuerySingle(@"
            INSERT INTO file (data) VALUES (@data);
            SELECT data FROM file WHERE file_id = LAST_INSERT_ROWID();",
            new("data", largeData),
            rd => rd.ReadBytes("data"));

        Assert.Equal(largeData, result);
    }

    [Fact]
    public void ReadString_ShouldHandleEmptyString() {
        var sql = "SELECT '' AS empty_string";

        using var conn = testDb.CreateConnection();
        var result = conn.QuerySingle(sql, rd => rd.ReadString("empty_string"));

        Assert.Equal(string.Empty, result);
    }

    [Fact]
    public void ReadDateTime_ShouldHandleEdgeCases() {
        var minDate = DateTime.MinValue;
        var maxDate = DateTime.MaxValue;

        var sql = @"
            SELECT @minDate AS min_date,
                   @maxDate AS max_date";

        var param = new DbParams
        {
            { "minDate", minDate },
            { "maxDate", maxDate }
        };

        using var conn = testDb.CreateConnection();
        var result = conn.QuerySingle(sql, param, rd => {
            Assert.Equal(minDate, rd.ReadDateTime("min_date"));
            Assert.Equal(maxDate, rd.ReadDateTime("max_date"));
            return 1;
        });

        Assert.Equal(1, result);
    }

    [Fact]
    public void ReadGuid_ShouldHandleEmptyGuid() {
        var sql = "SELECT @emptyGuid AS empty_guid";

        var param = new DbParams
        {
            { "emptyGuid", Guid.Empty }
        };

        using var conn = testDb.CreateConnection();
        var result = conn.QuerySingle(sql, param, rd => rd.ReadGuid("empty_guid"));

        Assert.Equal(Guid.Empty, result);
    }

    [Fact]
    public void ReadNullableDateOnly_ShouldHandleNullAndValidValues() {
        var nowDateOnly = DateOnly.FromDateTime(DateTime.Now);

        var sql = @"
            SELECT NULL AS null_date,
                   @validDate AS valid_date";

        var param = new DbParams
        {
            { "validDate", nowDateOnly }
        };

        using var conn = testDb.CreateConnection();
        var result = conn.QuerySingle(sql, param, rd => {
            Assert.Null(rd.ReadNullableDateOnly("null_date"));
            Assert.Equal(nowDateOnly, rd.ReadNullableDateOnly("valid_date"));
            return 1;
        });

        Assert.Equal(1, result);
    }

    [Fact]
    public void ReadNullableBoolean_ShouldHandleTrueAndFalse() {
        var sql = @"
            SELECT 1 AS true_value,
                   0 AS false_value,
                   NULL AS null_value";

        using var conn = testDb.CreateConnection();
        var result = conn.QuerySingle(sql, rd => {
            Assert.True(rd.ReadNullableBoolean("true_value"));
            Assert.False(rd.ReadNullableBoolean("false_value"));
            Assert.Null(rd.ReadNullableBoolean("null_value"));
            return 1;
        });

        Assert.Equal(1, result);
    }


    [Fact]
    public void ReadInvalidCast_ShouldThrowCouldNotCastValueException() {
        // due to SQLite's dynamic typing, we simulate a cast error using a mock IDataRecord
        var rd = new MockFailingDataRecord();
        var exception = Assert.Throws<DatabaseReadFieldException>(() => {
            rd.ReadInt32("int_field");
        });
        Assert.Equal(DatabaseErrorCode.CouldNotCastValue, exception.ErrorCode);
        Assert.Contains("int_field", exception.Message);
    }

#pragma warning disable CS8767 // Nullability of reference types in type of parameter doesn't match overridden member.
    private sealed class MockFailingDataRecord : IDataRecord {
        public int GetOrdinal(string name) {
            if (name == "int_field") return 0;
            throw new IndexOutOfRangeException();
        }

        public int GetInt32(int i) {
            if (i == 0) throw new InvalidCastException("Simulated cast exception");
            throw new IndexOutOfRangeException();
        }

        // Other members throw NotImplementedException
        public object this[int i] => throw new NotImplementedException();
        public object this[string name] => throw new NotImplementedException();
        public int FieldCount => throw new NotImplementedException();
        public bool GetBoolean(int i) => throw new NotImplementedException();
        public byte GetByte(int i) => throw new NotImplementedException();
        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => throw new NotImplementedException();
        public char GetChar(int i) => throw new NotImplementedException();
        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => throw new NotImplementedException();
        public IDataReader GetData(int i) => throw new NotImplementedException();
        public string GetDataTypeName(int i) => throw new NotImplementedException();
        public DateTime GetDateTime(int i) => throw new NotImplementedException();
        public decimal GetDecimal(int i) => throw new NotImplementedException();
        public double GetDouble(int i) => throw new NotImplementedException();
        public Type GetFieldType(int i) => throw new NotImplementedException();
        public float GetFloat(int i) => throw new NotImplementedException();
        public Guid GetGuid(int i) => throw new NotImplementedException();
        public short GetInt16(int i) => throw new NotImplementedException();
        public long GetInt64(int i) => throw new NotImplementedException();
        public string GetName(int i) => throw new NotImplementedException();
        public int GetValues(object[] values) => throw new NotImplementedException();
        public bool IsDBNull(int i) => throw new NotImplementedException();
        public string GetString(int i) => throw new NotImplementedException();
        public object GetValue(int i) => throw new NotImplementedException();
    }
#pragma warning restore CS8767 // Nullability of reference types in type of parameter doesn't match overridden member.
}
