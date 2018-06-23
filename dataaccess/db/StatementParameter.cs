using System;
using System.Collections.Generic;
using System.Text;

namespace es.dmoreno.utils.dataaccess.db
{
    public class StatementParameter
    {
        private string _param;
        private ParamType _type;
        private object _value;

        public string Nombre { get { return this._param; } }
        public ParamType Tipo { get { return this._type; } }
        public object Valor { get { return this._value; } }

        public StatementParameter(string name, ParamType type, object value)
        {
            this._param = name;
            this._type = type;
            this._value = value;
        }
    }
}
