namespace Leger
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;

    /// <summary>
    /// Represents a database parameter.
    /// </summary>
    public sealed class DbParams : Dictionary<string, object>
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
        public DbParams(string key, object value)
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
    public sealed class DbTypeParam
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DbTypeParam"/> class.
        /// </summary>
        /// <param name="dbType"></param>
        /// <param name="value"></param>
        public DbTypeParam(DbType dbType, object? value = null)
        {
            DbType = dbType;
            Value = value ?? DBNull.Value;
        }
        /// <summary> Gets the database type. </summary>
        public DbType DbType { get; }

        /// <summary> Gets the value. </summary>
        public object Value { get; }

        /// <summary> Gets the value as an AnsiString </summary>
        public static DbTypeParam AnsiString(string v) =>
            new DbTypeParam(DbType.AnsiString, v);

        /// <summary> Gets the value as a byte array. </summary>
        public static DbTypeParam Binary(byte[] v) =>
            new DbTypeParam(DbType.Binary, v);

        /// <summary> Gets the value as a byte array. </summary>
        public static DbTypeParam Bytes(byte[] v) =>
            Binary(v);

        /// <summary> Gets the value as a byte. </summary>
        public static DbTypeParam Byte(byte v) =>
            new DbTypeParam(DbType.Byte, v);

        /// <summary> Gets the value as a boolean. </summary>
        public static DbTypeParam Boolean(bool v) =>
            new DbTypeParam(DbType.Boolean, v);

        /// <summary> Gets the value as a currency. </summary>
        public static DbTypeParam Currency(decimal v) =>
            new DbTypeParam(DbType.Currency, v);
#if NET6_0_OR_GREATER
        /// <summary> Gets the value as a date. </summary>
        public static DbTypeParam Date(DateOnly v) =>
            new DbTypeParam(DbType.Date, v);
#endif
        /// <summary> Gets the value as a date time. </summary>
        public static DbTypeParam DateTime(DateTime v) =>
            new DbTypeParam(DbType.DateTime, v);

        /// <summary> Gets the value as a decimal. </summary>
        public static DbTypeParam Decimal(decimal v) =>
            new DbTypeParam(DbType.Decimal, v);

        /// <summary> Gets the value as a double. </summary>
        public static DbTypeParam Double(double v) =>
            new DbTypeParam(DbType.Double, v);

        /// <summary> Gets the value as a guid. </summary>
        public static DbTypeParam Guid(Guid v) =>
            new DbTypeParam(DbType.Guid, v);

        /// <summary> Gets the value as a short. </summary>
        public static DbTypeParam Int16(short v) =>
            new DbTypeParam(DbType.Int16, v);

        /// <summary> Gets the value as an integer. </summary>
        public static DbTypeParam Int32(int v) =>
            new DbTypeParam(DbType.Int32, v);

        /// <summary> Gets the value as a long. </summary>
        public static DbTypeParam Int64(long v) =>
            new DbTypeParam(DbType.Int64, v);

        /// <summary> Gets the value as an object. </summary>
        public static DbTypeParam Object(object v) =>
            new DbTypeParam(DbType.Object, v);

        /// <summary> Gets the value as a float. </summary>
        public static DbTypeParam Float(float v) =>
            new DbTypeParam(DbType.Single, v);

        /// <summary> Gets the value as a string. </summary>
        public static DbTypeParam String(string v) =>
            new DbTypeParam(DbType.String, v);

        /// <summary> Gets the value as a time. </summary>
        public static DbTypeParam UInt16(ushort v) =>
            new DbTypeParam(DbType.UInt16, v);

        /// <summary> Gets the value as an unsigned integer. </summary>
        public static DbTypeParam UInt32(uint v) =>
            new DbTypeParam(DbType.UInt32, v);

        /// <summary> Gets the value as an unsigned long. </summary>
        public static DbTypeParam UInt64(ulong v) =>
            new DbTypeParam(DbType.UInt64, v);

        /// <summary> Gets null AnsiString. </summary>
        public static DbTypeParam NullAnsiString =>
            new DbTypeParam(DbType.AnsiString);

        /// <summary> Gets null binary. </summary>
        public static DbTypeParam NullBinary =>
            new DbTypeParam(DbType.Binary);

        /// <summary> Gets null bytes. </summary>
        public static DbTypeParam NullBytes =>
            NullBinary;

        /// <summary> Gets null byte. </summary>
        public static DbTypeParam NullByte =>
            new DbTypeParam(DbType.Byte);

        /// <summary> Gets null boolean. </summary>
        public static DbTypeParam NullBoolean =>
            new DbTypeParam(DbType.Boolean);

        /// <summary> Gets null currency. </summary>
        public static DbTypeParam NullCurrency =>
            new DbTypeParam(DbType.Currency);

        /// <summary> Gets null date. </summary>
        public static DbTypeParam NullDate =>
            new DbTypeParam(DbType.Date);

        /// <summary> Gets null date time. </summary>
        public static DbTypeParam NullDateTime =>
            new DbTypeParam(DbType.DateTime);

        /// <summary> Gets null decimal. </summary>
        public static DbTypeParam NullDecimal =>
            new DbTypeParam(DbType.Decimal);

        /// <summary> Gets null double. </summary>
        public static DbTypeParam NullDouble =>
            new DbTypeParam(DbType.Double);

        /// <summary> Gets null guid. </summary>
        public static DbTypeParam NullGuid =>
            new DbTypeParam(DbType.Guid);

        /// <summary> Gets null short. </summary>
        public static DbTypeParam NullInt16 =>
            new DbTypeParam(DbType.Int16);

        /// <summary> Gets null integer. </summary>
        public static DbTypeParam NullInt32 =>
            new DbTypeParam(DbType.Int32);

        /// <summary> Gets null long. </summary>
        public static DbTypeParam NullInt64 =>
            new DbTypeParam(DbType.Int64);

        /// <summary> Gets null object. </summary>
        public static DbTypeParam NullObject =>
            new DbTypeParam(DbType.Object);

        /// <summary> Gets null float. </summary>
        public static DbTypeParam NullFloat =>
            new DbTypeParam(DbType.Single);

        /// <summary> Gets null string. </summary>
        public static DbTypeParam NullString =>
            new DbTypeParam(DbType.String);

        /// <summary> Gets null time. </summary>
        public static DbTypeParam NullTime =>
            new DbTypeParam(DbType.Time);

        /// <summary> Gets null unsigned short. </summary>
        public static DbTypeParam NullUInt16 =>
            new DbTypeParam(DbType.UInt16);

        /// <summary> Gets null unsigned integer. </summary>
        public static DbTypeParam NullUInt32 =>
            new DbTypeParam(DbType.UInt32);

        /// <summary> Gets null unsigned long. </summary>
        public static DbTypeParam NullUInt64 =>
            new DbTypeParam(DbType.UInt64);
    }
}
