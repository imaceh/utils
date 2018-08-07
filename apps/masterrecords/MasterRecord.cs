using es.dmoreno.utils.dataaccess.db;
using es.dmoreno.utils.serialize;
using System;
using System.Collections.Generic;
using System.Text;

namespace es.dmoreno.utils.apps.masterrecords
{
    public abstract class MasterRecord
    {
        [Field(FieldName = "id", IsAutoincrement = true, IsPrimaryKey = true, Type = ParamType.Int32)]
        public int ID { get; set; }

        [Field(FieldName = "key", Type = ParamType.String, Size = new int[] { 50 })]
        public string Key { get; set; }

        [Field(FieldName = "value", Type = ParamType.LongString)]
        public string Value { get; set; }

        public bool ValueBool
        {
            get
            {
                if (this.Value == null)
                {
                    return false;
                }
                else
                {
                    return this.Value == "1";
                }
            }

            set
            {
                if (value)
                {
                    this.Value = "1";
                }
                else
                {
                    this.Value = "0";
                }
            }
        }

        public int ValueInteger32
        {
            get
            {
                if (this.Value == null)
                {
                    return 0;
                }
                else
                {
                    int v;

                    try
                    {
                        v = Convert.ToInt32(this.Value);
                    }
                    catch
                    {
                        v = 0;
                    }

                    return v;
                }
            }

            set
            {
                this.Value = Convert.ToString(value);
            }
        }

        public T getComplexValue<T>() where T : class
        {
            T v;

            if (string.IsNullOrEmpty(this.Value))
            {
                v = null;
            }
            else
            {
                v = JSon.deserializeJSON<T>(this.Value);
            }

            return v;
        }

        public void setComplexValue<T>(T value) where T : class
        {
            this.Value = JSon.serializeJSON<T>(value);
        }
    }
}
