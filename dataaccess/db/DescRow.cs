using System;
using System.Collections.Generic;
using System.Text;

namespace es.dmoreno.utils.dataaccess.db
{
    public class DescRow
    {
        [Field(FieldName = "Field", AllowNull = true, Type = ParamType.String)]
        public string Field { get; set; }

        [Field(FieldName = "Type", AllowNull = true, Type = ParamType.String)]
        public string Type { get; set; }

        [Field(FieldName = "Null", AllowNull = true, Type = ParamType.String)]
        public string Null { get; set; }

        [Field(FieldName = "Key", AllowNull = true, Type = ParamType.String)]
        public string Key { get; set; }

        [Field(FieldName = "Default", AllowNull = true, Type = ParamType.String)]
        public string Default { get; set; }

        [Field(FieldName = "Extra", AllowNull = true, Type = ParamType.String)]
        public string Extra { get; set; }
    }
}
