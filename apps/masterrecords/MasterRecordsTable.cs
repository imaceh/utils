using es.dmoreno.utils.dataaccess.db;
using es.dmoreno.utils.serialize;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace es.dmoreno.utils.apps.masterrecords
{
    public class MasterRecordsTable<T> where T : MasterRecord, new()
    {
        private ConnectionParameters _params;

        protected MasterRecordsTable(ConnectionParameters param)
        {
            this._params = param;
        }

        public void createDBSchema()
        {
            using (DataBaseLogic db = new DataBaseLogic(this._params))
            {
                db.Management.createAlterTableAsync<T>();

                if (this._params.BeginTransaction)
                {
                    db.Statement.acceptTransaction();
                }
            }
        }

        public async Task<T> get(string key)
        {
            T v;

            using (DataBaseLogic db = new DataBaseLogic(this._params))
            {
                v = await db.Statement.firstAsync<T>(" AND key = @k", null, new List<StatementParameter>() {
                    new StatementParameter("@k", ParamType.String, key)
                });

                if (this._params.BeginTransaction)
                {
                    db.Statement.acceptTransaction();
                }
            }

            return v;
        }

        public async Task set(T record)
        {
            using (DataBaseLogic db = new DataBaseLogic(this._params))
            {
                var register = await db.Statement.firstAsync<T>(" AND key = @k", null, new List<StatementParameter>() {
                    new StatementParameter("@k", ParamType.String, record.Key)
                });

                if (register == null)
                {
                    await db.Statement.insertAsync(record);
                }
                else
                {
                    register.Value = record.Value;
                    await db.Statement.updateAsync(register);
                }

                if (this._params.BeginTransaction)
                {
                    db.Statement.acceptTransaction();
                }
            }
        }
    }
}
