using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace es.dmoreno.utils.dataaccess.db
{
    internal class MySQLManagement : Management
    {
        internal MySQLManagement(SQLStatement s) : base(s)
        {
        }

        public override async Task<bool> createAlterTableAsync<T>()
        {
            T t;
            List<FieldAttribute> pks_from_T;
            TableAttribute table_att;
            FieldAttribute field_att;
            List<string> pks;
            List<DescRow> desc_table;
            List<DescRow> pks_original;
            List<DescRow> pks_original_w_autoincrement;
            bool result;
            bool new_table;
            bool new_pk;
            bool add_autoinc;
            string sql;

            this.checkSchemaInMySQL<T>();

            result = true;

            t = new T();
            pks = new List<string>();

            //check if table exists
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

            //if not exists create a table empty with primary key            
            if (new_table)
            {
                sql = "CREATE TABLE " + table_att.Name + " ( _auto_created INT DEFAULT NULL) ";

                switch (table_att.Type)
                {
                    case EngineType.InnoDB:
                        sql += " ENGINE = INNODB";
                        break;
                    case EngineType.MyISAM:
                        sql += " ENGINE = MYISAM";
                        break;
                }

                await this.Statement.executeNonQueryAsync(sql);
            }

            //Create new fields
            desc_table = await this.getDescAsync<T>();

            foreach (PropertyInfo item in t.GetType().GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
            {
                field_att = item.GetCustomAttribute<FieldAttribute>();
                if (field_att != null)
                {
                    //if no exists then will be created
                    if (desc_table.Where(f => f.Field == field_att.FieldName).Count() == 0)
                    {
                        sql = "ALTER TABLE " + table_att.Name + " ADD COLUMN " + this.getCreateFieldMySQL(field_att);
                        await this.Statement.executeNonQueryAsync(sql);
                    }
                }
            }

            //Compare PKs for decide if will be update PK constraint
            pks_original = desc_table.Where(r => r.Key == "PRI").ToList();
            pks_from_T = this.getPrimariesKeys<T>();

            new_pk = pks_original.Count != pks_from_T.Count;

            if (!new_pk)
            {
                foreach (DescRow item in pks_original)
                {
                    if (pks_from_T.Where(f => f.FieldName == item.Field).Count() == 0)
                    {
                        new_pk = true;
                        break;
                    }
                }
            }

            //Get actual autoincrement field
            pks_original_w_autoincrement = pks_original.Where(f => f.Extra.Contains("auto_increment")).ToList();

            //Remove AUTO_INCREMENT field
            if (pks_original_w_autoincrement.Count > 0)
            {
                //No same autoincrement field
                if ((pks_from_T.Where(f => pks_original_w_autoincrement[0].Field == f.FieldName).Count() == 0) ||
                    //Not exists autoincrement in new schema
                    (pks_from_T.Where(f => f.IsAutoincrement == true).Count() == 0))
                {
                    add_autoinc = true;

                    //Remove actual attribute from actual auto_criment field
                    sql = "ALTER TABLE " + table_att.Name + " MODIFY COLUMN " + pks_original_w_autoincrement[0].Field + " " + pks_original_w_autoincrement[0].Type;
                    if (pks_original_w_autoincrement[0].Null == "NO")
                    {
                        sql += " NOT ";
                    }
                    sql += " NULL";
                    if (pks_original_w_autoincrement[0].Default != null)
                    {
                        sql += " DEFAULT '" + pks_original_w_autoincrement[0].Default + "'";
                    }

                    await this.Statement.executeNonQueryAsync(sql);
                }
                else
                {
                    add_autoinc = false;
                }
            }
            else
            {
                add_autoinc = true;
            }

            //If is necessary create new PK
            if (new_pk)
            {
                if (pks_original.Count > 0)
                {
                    sql = "ALTER TABLE " + table_att.Name + " DROP PRIMARY KEY";
                    await this.Statement.executeNonQueryAsync(sql);
                }

                if (pks_from_T.Count > 0)
                {
                    sql = "ALTER TABLE " + table_att.Name + " ADD CONSTRAINT pk_" + table_att.Name + " PRIMARY KEY (";
                    for (int i = 0; i < pks_from_T.Count; i++)
                    {
                        sql += pks_from_T[i].FieldName;

                        if (i < pks_from_T.Count - 1)
                        {
                            sql += ", ";
                        }
                    }
                    sql += ")";

                    await this.Statement.executeNonQueryAsync(sql);
                }
            }

            //If is necessary create new autoincrement field
            if (add_autoinc)
            {
                foreach (FieldAttribute item in pks_from_T)
                {
                    if (item.IsAutoincrement)
                    {
                        if (item.isInteger)
                        {
                            sql = "ALTER TABLE " + table_att.Name + " MODIFY COLUMN " + this.getCreateFieldMySQL(item) + " AUTO_INCREMENT";
                            await this.Statement.executeNonQueryAsync(sql);
                        }
                        break;
                    }
                }
            }

            result = true;

            return result;
        }

        public override async Task<List<DescRow>> getDescAsync<T>()
        {
            SQLData data;
            TableAttribute table_att;

            table_att = new T().GetType().GetTypeInfo().GetCustomAttribute<TableAttribute>();

            data = await this.Statement.executeAsync("DESC " + table_att.Name);

            return data.fillToList<DescRow>();
        }

        private string getCreateFieldMySQL(FieldAttribute field_info, bool without_pk = false)
        {
            string result;

            result = field_info.FieldName + " " + this.Statement.Conector.getTypeString(field_info.Type, field_info.Size);

            if (!field_info.IsPrimaryKey || (field_info.IsPrimaryKey && without_pk))
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
                else if (field_info.Type == ParamType.String || field_info.Type == ParamType.LongString)
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

        private void checkSchemaInMySQL<T>() where T : class, new()
        {
            FieldAttribute field_att;
            int auto_increment_fields = 0;

            foreach (PropertyInfo item in new T().GetType().GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
            {
                field_att = item.GetCustomAttribute<FieldAttribute>();
                if (field_att != null)
                {
                    if (field_att.IsAutoincrement)
                    {
                        auto_increment_fields++;

                        if (!field_att.isInteger)
                        {
                            throw new Exception("Field " + field_att.FieldName + " not is integer field but has enable auto_increment attributte");
                        }
                    }
                }
            }

            if (auto_increment_fields > 1)
            {
                throw new Exception("Only is allowed one auto_increment field");
            }
        }
    }
}
