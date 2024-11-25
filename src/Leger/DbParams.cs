namespace Leger;

using System.Data;

/// <summary>
/// Represents a database parameter.
/// </summary>
public sealed class DbParams : Dictionary<string, object?>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DbParams"/> class.
    /// </summary>
    public DbParams()
    {
    }

    /// <summary>
    ///  Initializes a new instance of the <see cref="DbParams"/> class.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    public DbParams(string key, object? value)
    {
        if (!this.ContainsKey(key))
        {
            this[key] = value;
        }
    }
}

/// <summary>
/// Represents a database parameter extensions.
/// </summary>
public static class DbParamsExtensions
{
    /// <summary>
    /// Adds a parameter to the <see cref="DbParams"/>.
    /// </summary>
    /// <param name="p1"></param>
    /// <param name="p2"></param>
    /// <returns></returns>
    public static DbParams Add(this DbParams p1, DbParams p2)
    {
        p2.ToList()
          .ForEach(x =>
          {
              if (!p1.ContainsKey(x.Key))
              {
                  p1.Add(x.Key, x.Value);
              }
          });

        return p1;
    }
}

/// <summary>
/// Represents a database parameter type.
/// </summary>
/// <param name="dbType"></param>
/// <param name="value"></param>
public sealed class DbTypeParam(DbType dbType, object? value = null)
{
    /// <summary> Gets the database type. </summary>
    public DbType DbType { get; } = dbType;

    /// <summary> Gets the value. </summary>
    public object Value { get; } = value ?? DBNull.Value;

    /// <summary> Gets the value as an AnsiString </summary>
    public static DbTypeParam AnsiString(string v) =>
        new(DbType.AnsiString, v);

    /// <summary> Gets the value as a byte array. </summary>
    public static DbTypeParam Binary(byte[] v) =>
        new(DbType.Binary, v);

    /// <summary> Gets the value as a byte array. </summary>
    public static DbTypeParam Bytes(byte[] v) =>
        Binary(v);

    /// <summary> Gets the value as a byte. </summary>
    public static DbTypeParam Byte(byte v) =>
        new(DbType.Byte, v);

    /// <summary> Gets the value as a boolean. </summary>
    public static DbTypeParam Boolean(bool v) =>
        new(DbType.Boolean, v);

    /// <summary> Gets the value as a currency. </summary>
    public static DbTypeParam Currency(decimal v) =>
        new(DbType.Currency, v);

    /// <summary> Gets the value as a date. </summary>
    public static DbTypeParam Date(DateOnly v) =>
        new(DbType.Date, v);

    /// <summary> Gets the value as a date time. </summary>
    public static DbTypeParam DateTime(DateTime v) =>
        new(DbType.DateTime, v);

    /// <summary> Gets the value as a decimal. </summary>
    public static DbTypeParam Decimal(decimal v) =>
        new(DbType.Decimal, v);

    /// <summary> Gets the value as a double. </summary>
    public static DbTypeParam Double(double v) =>
        new(DbType.Double, v);

    /// <summary> Gets the value as a guid. </summary>
    public static DbTypeParam Guid(Guid v) =>
        new(DbType.Guid, v);

    /// <summary> Gets the value as a short. </summary>
    public static DbTypeParam Int16(short v) =>
        new(DbType.Int16, v);

    /// <summary> Gets the value as an integer. </summary>
    public static DbTypeParam Int32(int v) =>
        new(DbType.Int32, v);

    /// <summary> Gets the value as a long. </summary>
    public static DbTypeParam Int64(long v) =>
        new(DbType.Int64, v);

    /// <summary> Gets the value as an object. </summary>
    public static DbTypeParam Object(object v) =>
        new(DbType.Object, v);

    /// <summary> Gets the value as a float. </summary>
    public static DbTypeParam Float(float v) =>
        new(DbType.Single, v);

    /// <summary> Gets the value as a string. </summary>
    public static DbTypeParam String(string v) =>
        new(DbType.String, v);

    /// <summary> Gets the value as a time. </summary>
    public static DbTypeParam UInt16(ushort v) =>
        new(DbType.UInt16, v);

    /// <summary> Gets the value as an unsigned integer. </summary>
    public static DbTypeParam UInt32(uint v) =>
        new(DbType.UInt32, v);

    /// <summary> Gets the value as an unsigned long. </summary>
    public static DbTypeParam UInt64(ulong v) =>
        new(DbType.UInt64, v);

    /// <summary> Gets null AnsiString. </summary>
    public static DbTypeParam NullAnsiString =>
        new(DbType.AnsiString);

    /// <summary> Gets null binary. </summary>
    public static DbTypeParam NullBinary =>
        new(DbType.Binary);

    /// <summary> Gets null bytes. </summary>
    public static DbTypeParam NullBytes =>
        NullBinary;

    /// <summary> Gets null byte. </summary>
    public static DbTypeParam NullByte =>
        new(DbType.Byte);

    /// <summary> Gets null boolean. </summary>
    public static DbTypeParam NullBoolean =>
        new(DbType.Boolean);

    /// <summary> Gets null currency. </summary>
    public static DbTypeParam NullCurrency =>
        new(DbType.Currency);

    /// <summary> Gets null date. </summary>
    public static DbTypeParam NullDate =>
        new(DbType.Date);

    /// <summary> Gets null date time. </summary>
    public static DbTypeParam NullDateTime =>
        new(DbType.DateTime);

    /// <summary> Gets null decimal. </summary>
    public static DbTypeParam NullDecimal =>
        new(DbType.Decimal);

    /// <summary> Gets null double. </summary>
    public static DbTypeParam NullDouble =>
        new(DbType.Double);

    /// <summary> Gets null guid. </summary>
    public static DbTypeParam NullGuid =>
        new(DbType.Guid);

    /// <summary> Gets null short. </summary>
    public static DbTypeParam NullInt16 =>
        new(DbType.Int16);

    /// <summary> Gets null integer. </summary>
    public static DbTypeParam NullInt32 =>
        new(DbType.Int32);

    /// <summary> Gets null long. </summary>
    public static DbTypeParam NullInt64 =>
        new(DbType.Int64);

    /// <summary> Gets null object. </summary>
    public static DbTypeParam NullObject =>
        new(DbType.Object);

    /// <summary> Gets null float. </summary>
    public static DbTypeParam NullFloat =>
        new(DbType.Single);

    /// <summary> Gets null string. </summary>
    public static DbTypeParam NullString =>
        new(DbType.String);

    /// <summary> Gets null time. </summary>
    public static DbTypeParam NullTime =>
        new(DbType.Time);

    /// <summary> Gets null unsigned short. </summary>
    public static DbTypeParam NullUInt16 =>
        new(DbType.UInt16);

    /// <summary> Gets null unsigned integer. </summary>
    public static DbTypeParam NullUInt32 =>
        new(DbType.UInt32);

    /// <summary> Gets null unsigned long. </summary>
    public static DbTypeParam NullUInt64 =>
        new(DbType.UInt64);
}
