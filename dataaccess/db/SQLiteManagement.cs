using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Linq;
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
            bool result;
            bool new_table;
            string sql;

            this.checkSchemaSQLite<T>();

            result = true;

            t = new T();
            
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
                var pks = Utils.getFieldAttributes(Utils.getPropertyInfos<T>(t, true)).Where(a => a.IsPrimaryKey).ToList();

                sql = "CREATE TABLE " + table_att.Name + " (";
                if (pks.Count == 0)
                {
                    sql += " _auto_created INTEGER DEFAULT NULL";
                }
                else if (pks.Count == 1)
                {
                    sql += " " + this.getCreateFieldSQLite(pks[0], true);
                }
                else
                {
                    for (int i = 0; i < pks.Count; i++)
                    {
                        sql += this.getCreateFieldSQLite(pks[i], false) + ", ";
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

                sql += await this.createUpdateConstraint<T>();

                sql += ")";

                await this.Statement.executeNonQueryAsync(sql);
            }

            //Check if fields exists
            foreach(var item in Utils.getFieldAttributes(Utils.getPropertyInfos<T>(t, true)).Where(a => !a.IsPrimaryKey))
            {
                //Check if exists
                sql = "SELECT " + item.FieldName + " FROM " + table_att.Name + " LIMIT 1";
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
                    sql = "ALTER TABLE " + table_att.Name + " ADD COLUMN " + this.getCreateFieldSQLite(item, false);
                    await this.Statement.executeNonQueryAsync(sql);
                    result = true;
                }
            }

            

            return result;
        }

        public override Task<List<DescRow>> getDescAsync<T>()
        {
            throw new NotImplementedException();
        }

        internal string getCreateFieldSQLite(FieldAttribute field_info, bool include_pk = false)
        {
            string result;

            result = field_info.FieldName + " " + this.Statement.getTypeSQLiteString(field_info.Type);

            if (include_pk)
            {
                if (field_info.IsPrimaryKey)
                {
                    result += " PRIMARY KEY";
                }
            }

            if (field_info.IsAutoincrement && field_info.isNumeric)
            {
                result += " AUTOINCREMENT";
            }

            if (!field_info.IsPrimaryKey || (field_info.IsPrimaryKey && !include_pk))
            {
                if (!field_info.AllowNull)
                {
                    result += " NOT NULL";
                }
            }

            if (field_info.DefaultValue != null)
            {
                if (field_info.Type == ParamType.Boolean)
                {
                    if ((bool)field_info.DefaultValue)
                    {
                        result += " DEFAULT 1";
                    }
                    else
                    {
                        result += " DEFAULT 0";
                    }
                }
                else if (field_info.Type == ParamType.Int16 || field_info.Type == ParamType.Int32 || field_info.Type == ParamType.Int64)
                {
                    result += " DEFAULT " + Convert.ToInt32(field_info.DefaultValue).ToString();
                }
                else if (field_info.Type == ParamType.DateTime)
                {
                    if ((string)field_info.DefaultValue == "0")
                    {
                        result += " DEFAULT " + DateTime.MinValue.Ticks.ToString();
                    }
                    else
                    {
                        result += " DEFAULT " + DateTime.Parse((string)field_info.DefaultValue).Ticks.ToString();
                    }
                }
                else if (field_info.Type == ParamType.String)
                {
                    result += " DEFAULT '" + (string)field_info.DefaultValue + "'";
                }
                else if (field_info.Type == ParamType.Decimal)
                {
                    result += " DEFAULT " + Convert.ToDecimal(field_info.DefaultValue).ToString();
                }
                else
                {
                    throw new Exception("Datatype not supported");
                }
            }

            return result;
        }

        private void checkSchemaSQLite<T>() where T : class, new()
        {
            var attributes = Utils.getFieldAttributes(Utils.getPropertyInfos<T>(new T(), true));

            var pks = 0;
            var ai = 0;
            var pks_ai = 0;

            foreach (var item in attributes)
            {
                if (item.IsAutoincrement)
                {
                    ai++;

                    if (!item.isNumeric)
                    {
                        throw new Exception("Field " + item.FieldName + " is has AUTOINCREMENT attribute but not is numeric");
                    }
                }

                if (item.IsPrimaryKey)
                {
                    pks++;
                }

                if (item.IsAutoincrement && item.IsPrimaryKey)
                {
                    pks_ai++;
                }
            }

            if (ai > 1)
            {
                throw new Exception("The use of the AUTOINCREMENT attribute in more than one field is not allowed");
            }

            if ((pks_ai == 1) && (pks > 1))
            {
                throw new Exception("The use of the AUTOINCREMENT attribute in primary key field when exists primary key combined is not allowed");
            }
        }

        private async Task<string> createUpdateConstraint<T>() where T: class, new()
        {
            var reg = new T();
            var table_att = reg.GetType().GetTypeInfo().GetCustomAttribute<TableAttribute>();

            //first get all constraints
            var properties = Utils.getPropertyInfos<T>(reg, false);
            var cons = Utils.getFieldConstraints(properties).ToList();

            //agroup constraints by name
            var cons_availables = cons.GroupBy(c => c.Name).ToList();

            var sql = "";
            foreach (var item in cons_availables)
            {
                sql += ", ";
                if (item.ElementAt(0).Type == EConstraintType.ForeignKey)
                {
                    sql += " FOREIGN KEY (";
                }
                else
                {
                    sql += " UNIQUE KEY (";
                }

                var references = "";
                for (int i = 0; i < item.Count(); i++)
                {
                    sql += item.ElementAt(i).FieldName;
                    references += item.ElementAt(i).ReferencedField;

                    if (i < (item.Count() - 1))
                    {
                        sql += ", ";
                        references += ", ";
                    }
                }

                sql += ") REFERENCES " + item.ElementAt(0).ReferencedTable + " (" + references + ")";
            }

            return sql;
        }
    }
}
