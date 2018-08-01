using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace es.dmoreno.utils.dataaccess.db
{
    internal class SQLiteManagement : Management
    {
        internal SQLiteManagement(SQLStatement s) : base(s) { }

        public override async Task<bool> createAlterTableAsync<T>()
        {
            T t;
            TableAttribute table_att;
            FieldAttribute field_att;
            List<FieldAttribute> pks;
            bool result;
            bool new_table;
            string sql;

            result = true;

            t = new T();
            pks = new List<FieldAttribute>();

            //Check if table exists
            table_att = t.GetType().GetTypeInfo().GetCustomAttribute<TableAttribute>();

            try
            {
                sql = "SELECT * FROM " + table_att.Name + " LIMIT 1";
                await this.Statement.executeAsync(sql);
                result = true;
                new_table = false;
            }
            catch
            {
                result = false;
                new_table = true;
            }

            if (new_table)
            {
                foreach (PropertyInfo item in t.GetType().GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
                {
                    field_att = item.GetCustomAttribute<FieldAttribute>();
                    if (field_att != null)
                    {
                        if (field_att.IsPrimaryKey)
                        {
                            pks.Add(field_att);
                        }
                    }
                }

                sql = "CREATE TABLE " + table_att.Name + " (";
                if (pks.Count == 0)
                {
                    sql += " _auto_created INTEGER DEFAULT NULL";
                }
                else
                {
                    for (int i = 0; i < pks.Count; i++)
                    {
                        sql += this.Statement.getCreateFieldSQLite(pks[i]) + ", ";
                    }

                    sql += " PRIMARY KEY (";
                    for (int i = 0; i < pks.Count; i++)
                    {
                        sql += pks[i].FieldName;

                        if (i < pks.Count - 1)
                        {
                            sql += ", ";
                        }
                    }
                    sql += ")";
                }
                sql += ")";

                await this.Statement.executeNonQueryAsync(sql);
            }

            //Check if fields exists
            foreach (PropertyInfo item in t.GetType().GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
            {
                field_att = item.GetCustomAttribute<FieldAttribute>();

                if (field_att != null)
                {
                    if (!field_att.IsPrimaryKey)
                    {
                        //Check if exists
                        sql = "SELECT " + field_att.FieldName + " FROM " + table_att.Name + " LIMIT 1";
                        try
                        {
                            await this.Statement.executeAsync(sql);
                            result = true;
                        }
                        catch
                        {
                            result = false;
                        }

                        //Create field
                        if (!result)
                        {
                            sql = "ALTER TABLE " + table_att.Name + " ADD COLUMN " + this.Statement.getCreateFieldSQLite(field_att, true);
                            await this.Statement.executeNonQueryAsync(sql);
                            result = true;
                        }
                    }
                }
            }

            return result;
        }

        public override DescRow getDescAsync<T>()
        {
            throw new NotImplementedException();
        }
    }
}
