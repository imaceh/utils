using System;
using System.Collections.Generic;
using System.Text;

namespace es.dmoreno.utils.dataaccess.db
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true, Inherited = false)]
    public class ConstraintAttribute : Attribute
    {
        public EConstraintType Type { get; set; }
        public string Name { get; set; }
        public string ReferencedField { get; set; }
        public string ReferencedTable { get; set; }

        internal string FieldName { get; set; }
    }
}
