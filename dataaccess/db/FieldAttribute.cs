using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace es.dmoreno.utils.dataaccess.db
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = false)]
    public class FieldAttribute : Attribute
    {
        public string FieldName { get; set; }
        public bool AllowNull { get; set; }
        public bool IsPrimaryKey { get; set; }
        public bool IsAutoincrement { get; set; }
        public ParamType Type { get; set; }
        public int[] Size { get; set; }
        public object DefaultValue { get; set; }

        public int NullValueForInt { get; set; } = int.MinValue;

        public bool isInteger
        {
            get
            {
                return this.Type == ParamType.Int16 || this.Type == ParamType.Int32 || this.Type == ParamType.Int64;
            }
        }

        public bool isDecimal
        {
            get
            {
                return this.Type == ParamType.Decimal;
            }
        }

        public bool isNumeric
        {
            get
            {
                return this.isInteger || this.isDecimal;
            }
        }
    }
}
