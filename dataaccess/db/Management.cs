using System;
using System.Collections.Generic;
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

        public abstract DescRow getDescAsync<T>() where T : class, new();

        public abstract Task<bool> createAlterTableAsync<T>() where T : class, new();
    }
}
