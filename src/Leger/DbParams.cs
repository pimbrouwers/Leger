namespace Leger;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

public sealed class DbParams : Dictionary<string, object?>
{
    public DbParams()
    {
    }

    public DbParams(string key, object? value)
    {
        if (!this.ContainsKey(key))
        {
            this[key] = value;
        }
    }
}

public static class DbParamsExtensions
{

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

public sealed class DbTypeParam(DbType dbType, object? value = null)
{
    public DbType DbType { get; } = dbType;

    public object Value { get; } = value ?? DBNull.Value;

    public static DbTypeParam AnsiString(string v) =>
        new(DbType.AnsiString, v);

    public static DbTypeParam Binary(byte[] v) =>
        new(DbType.Binary, v);

    public static DbTypeParam Bytes(byte[] v) =>
        Binary(v);

    public static DbTypeParam Byte(byte v) =>
        new(DbType.Byte, v);

    public static DbTypeParam Boolean(bool v) =>
        new(DbType.Boolean, v);

    public static DbTypeParam Currency(decimal v) =>
        new(DbType.Currency, v);

    public static DbTypeParam Date(DateTime v) =>
        new(DbType.Date, v);

    public static DbTypeParam DateTime(DateTime v) =>
        new(DbType.DateTime, v);

    public static DbTypeParam Decimal(decimal v) =>
        new(DbType.Decimal, v);

    public static DbTypeParam Double(double v) =>
        new(DbType.Double, v);

    public static DbTypeParam Guid(Guid v) =>
        new(DbType.Guid, v);

    public static DbTypeParam Int16(short v) =>
        new(DbType.Int16, v);

    public static DbTypeParam Int32(int v) =>
        new(DbType.Int32, v);

    public static DbTypeParam Int64(long v) =>
        new(DbType.Int64, v);

    public static DbTypeParam Object(object v) =>
        new(DbType.Object, v);

    public static DbTypeParam Float(float v) =>
        new(DbType.Single, v);

    public static DbTypeParam String(string v) =>
        new(DbType.String, v);

    public static DbTypeParam UInt16(ushort v) =>
        new(DbType.UInt16, v);

    public static DbTypeParam UInt32(uint v) =>
        new(DbType.UInt32, v);

    public static DbTypeParam UInt64(ulong v) =>
        new(DbType.UInt64, v);

    public static DbTypeParam NullAnsiString =>
        new(DbType.AnsiString);

    public static DbTypeParam NullBinary =>
        new(DbType.Binary);

    public static DbTypeParam NullBytes =>
        NullBinary;

    public static DbTypeParam NullByte =>
        new(DbType.Byte);

    public static DbTypeParam NullBoolean =>
        new(DbType.Boolean);

    public static DbTypeParam NullCurrency =>
        new(DbType.Currency);

    public static DbTypeParam NullDate =>
        new(DbType.Date);

    public static DbTypeParam NullDateTime =>
        new(DbType.DateTime);

    public static DbTypeParam NullDecimal =>
        new(DbType.Decimal);

    public static DbTypeParam NullDouble =>
        new(DbType.Double);

    public static DbTypeParam NullGuid =>
        new(DbType.Guid);

    public static DbTypeParam NullInt16 =>
        new(DbType.Int16);

    public static DbTypeParam NullInt32 =>
        new(DbType.Int32);

    public static DbTypeParam NullInt64 =>
        new(DbType.Int64);

    public static DbTypeParam NullObject =>
        new(DbType.Object);

    public static DbTypeParam NullFloat =>
        new(DbType.Single);

    public static DbTypeParam NullString =>
        new(DbType.String);

    public static DbTypeParam NullTime =>
        new(DbType.Time);

    public static DbTypeParam NullUInt16 =>
        new(DbType.UInt16);

    public static DbTypeParam NullUInt32 =>
        new(DbType.UInt32);

    public static DbTypeParam NullUInt64 =>
        new(DbType.UInt64);
}
