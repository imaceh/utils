using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace es.dmoreno.utils.dataaccess.db
{
    public abstract class Management
    {
        private SQLStatement _s;

        protected SQLStatement Statement { get { return this._s; } }

        internal Management(SQLStatement s)
        {
            this._s = s;
        }

        public abstract Task<List<DescRow>> getDescAsync<T>() where T : class, new();

        public abstract Task<bool> createAlterTableAsync<T>() where T : class, new();

        protected List<FieldAttribute> getPrimariesKeys<T>() where T : class, new()
        {
            FieldAttribute field_att;
            List<FieldAttribute> result;

            result = new List<FieldAttribute>();

            foreach (PropertyInfo item in new T().GetType().GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
            {
                field_att = item.GetCustomAttribute<FieldAttribute>();

                if (field_att.IsPrimaryKey)
                {
                    result.Add(field_att);
                }
            }

            return result;
        }
    }
}
