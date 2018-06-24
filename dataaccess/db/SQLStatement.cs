﻿using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace es.dmoreno.utils.dataaccess.db
{
    public class SQLStatement : IDisposable
    {
        private class ConfigStatement
        {
            public bool isFirstPKAutoIncrementInt { get; set; } = false;
            public string SQL { get; set; }
            public string SELECT { get; set; }
            public List<StatementParameter> Params { get; set; }
        }

        public string LastError { get; internal set; }

        private SemaphoreSlim Semaphore { get; } = new SemaphoreSlim(1);
        private DbConnection _connection = null;
        private IConnector _connector = null;
        private DbCommand _command = null;
        private DbDataReader _datareader = null;
        private DbTransaction _transaction = null;
        private List<DbParameter> _parameters = new List<DbParameter>();
        private string _string_connection;
        private DBMSType sgbd;
        private bool disposed = false;
        private SQLData _result_data = null;
        private object _transaction_secure_object = null;
        private bool _transaction_secure_active = false;

        public DBMSType Type
        {
            get { return sgbd; }
        }

        public SQLStatement(string string_connection, DBMSType dbms)
        {
            this.initialize(string_connection, dbms, null);
        }

        public SQLStatement(string string_connection, DBMSType dbms, IConnector connector)
        {
            this.initialize(string_connection, dbms, connector);
        }

        private void initialize(string string_connection, DBMSType dbms, IConnector connector)
        {
            this._string_connection = string_connection;
            this.sgbd = dbms;
            this._connector = connector;

            if (dbmsTypeRequireConnector(dbms) && connector == null)
            {
                throw new Exception("DBMS selected require extern connector");
            }

            this.open();
        }

        static private bool dbmsTypeRequireConnector(DBMSType type)
        {
            switch (type)
            {
                case DBMSType.MySQL: return true;
                default: return false;
            }
        }

        private void createCommand()
        {
            this._command = this._connection.CreateCommand();
            this.loadParameters();
        }



        #region Transacciones seguras
        /// <summary>
        /// Genera una transacción de forma segura de forma que permite el uso de transacciones anidadas
        /// </summary>
        /// <param name="o"></param>
        public void beginTransaction(object o)
        {
            if (o == null)
                throw new Exception("No se ha indicado un objeto para iniciar una transacción segura");

            if (this._transaction_secure_object == null)
            {
                this._transaction_secure_object = o;

                if (this.transactionInProgress())
                {
                    this._transaction_secure_active = false;
                    return;
                }
                else
                {
                    this._transaction_secure_active = true;
                    this.beginTransaction();
                }
            }
        }

        /// <summary>
        /// Acepta una transacción segura
        /// </summary>
        /// <param name="o"></param>
        public void acceptTransaction(object o)
        {
            if (o == null)
                throw new Exception("No se ha indicado un objeto para terminar una transacción segura");

            if (this._transaction_secure_object == o)
            {
                if (this._transaction_secure_active)
                    this.acceptTransaction();
                this._transaction_secure_object = null;
            }
        }

        /// <summary>
        /// Rechaza una transacción segura
        /// </summary>
        /// <param name="o"></param>
        public void refuseTransaction(object o)
        {
            if (o == null)
                throw new Exception("No se ha indicado un objeto para terminar una transacción segura");

            if (this._transaction_secure_object == o)
            {
                if (this._transaction_secure_active)
                    this.refuseTransaction();
                this._transaction_secure_object = null;
            }
        }

        /// <summary>
        /// Muestra si la transacción seguro esta activa
        /// </summary>
        public bool isActiveSecureTransaction
        {
            get { return this._transaction_secure_active; }
        }
        #endregion

        #region *************Transacciones****************

        public bool transactionInProgress()
        {
            return this._transaction != null;
        }

        public void beginTransaction()
        {
            if (this._transaction == null)
            {
                if (this._connection != null)
                {
                    this.finalizeSqlData(); //finalizamos los objetos data que estan abiertos
                    this._transaction = this._connection.BeginTransaction();
                }
                else
                    throw new Exception("No existe ninguna conexión activa");
            }
            else
                throw new Exception("Existe una transacción en curso");
        }

        public void acceptTransaction()
        {
            if (this._transaction != null)
            {
                this.finalizeSqlData();
                this.empty();
                this._transaction.Commit();
                this._transaction = null;
            }
            else
                throw new Exception("No existe transacción en curso");
        }

        public void refuseTransaction()
        {
            if (this._transaction != null)
            {
                this.finalizeSqlData();
                this.empty();
                this._transaction.Rollback();
                this._transaction = null;
            }
            else
                throw new Exception("No existe transacción en curso");
        }

        #endregion

        public SQLData execute(string sql)
        {
            this.finalizeSqlData();
            this.createCommand();
            this._command.CommandText = sql;
            try
            {
                this._datareader = this._command.ExecuteReader();
            }
            catch (Exception ex)
            {
                this.LastError = ex.Message;
                throw ex;
            }
            SQLData d = new SQLData(this._datareader, this._command, this);
            this._result_data = d;
            this.empty();
            return d;
        }

        public async Task<SQLData> executeAsync(string sql)
        {
            this.finalizeSqlData();
            this.createCommand();
            this._command.CommandText = sql;
            try
            {
                this._datareader = await this._command.ExecuteReaderAsync();
            }
            catch (Exception ex)
            {
                this.LastError = ex.Message;
                throw ex;
            }
            SQLData d = new SQLData(this._datareader, this._command, this);
            this._result_data = d;
            this.empty();
            return d;
        }

        public int executeNonQuery(string sql)
        {
            int resultado;

            this.finalizeSqlData();
            this.createCommand();
            this._command.CommandText = sql;
            try
            {
                resultado = this._command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                this.LastError = ex.Message;
                throw ex;
            }
            this.empty();
            return resultado;
        }

        public async Task<int> executeNonQueryAsync(string sql)
        {
            int resultado;

            this.finalizeSqlData();
            this.createCommand();
            this._command.CommandText = sql;
            try
            {
                resultado = await this._command.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                this.LastError = ex.Message;
                throw ex;
            }

            this.empty();
            return resultado;
        }

        private void loadParameters()
        {
            foreach (var item in _parameters)
            {
                this._command.Parameters.Add(item);
            }
            this._parameters.Clear();
        }

        public void open()
        {
            if (string.IsNullOrEmpty(this._string_connection))
            {
                throw new Exception("String connection is empty");
            }

            this._connection = this.getConnection();
        }

        public void close()
        {
            this.finalizeSqlData();
            if (this._connection != null)
            {
                this._connection.Close();
                this._connection.Dispose();
                this._connection = null;
            }
        }

        private void finalizeSqlData()
        {
            if (this._result_data != null)
            {
                this._result_data.Dispose();
                this._result_data = null;
            }
        }

        private void empty()
        {
            this._command = null;
            this._datareader = null;
        }

        private DbConnection getConnection()
        {
            DbConnection con;

            switch (this.sgbd)
            {
                case DBMSType.MySQL:
                    con = this._connector.getConnection(this._string_connection);
                    break;
                case DBMSType.SQLite:
                    con = this.getConnectionSQLite();
                    break;
                default:
                    throw new Exception("Select a valid DBMS");
            }

            if (con.State == System.Data.ConnectionState.Closed)
            {
                con.Open();
            }

            return con;
        }

        private SqliteConnection getConnectionSQLite()
        {
            SqliteConnection con;

            con = new SqliteConnection(this._string_connection);
            con.Open();

            return con;
        }

        public int lastID
        {
            get
            {
                int id;

                if (this.sgbd == DBMSType.MySQL)
                {
                    SQLData d = this.execute("SELECT LAST_INSERT_ID() AS id;");
                    if (d.isEmpty())
                    {
                        id = 0;
                    }
                    else
                    {
                        d.next();
                        id = d.getInt32("id");
                    }
                }
                else if (this.sgbd == DBMSType.SQLite)
                {
                    using (SQLData d = this.execute("SELECT last_insert_rowid() AS id;"))
                    {
                        if (d.isEmpty())
                        {
                            id = 0;
                        }
                        else
                        {
                            d.next();
                            id = d.getInt32("id");
                        }
                    }
                }
                else
                {
                    throw new Exception("Can't get LastID with DBMS selected");
                }

                return id;
            }
        }

        #region ***************Manipulación de parámetros***************

        public void clearParameters()
        {
            this._parameters.Clear();
        }

        private SqliteType getTypeSQLite(ParamType type)
        {
            SqliteType t;
            switch (type)
            {
                case ParamType.Decimal:
                    t = SqliteType.Real;
                    break;
                case ParamType.String:
                    t = SqliteType.Text;
                    break;
                case ParamType.Int16:
                    t = SqliteType.Integer;
                    break;
                case ParamType.Int32:
                    t = SqliteType.Integer;
                    break;
                case ParamType.Int64:
                    t = SqliteType.Integer;
                    break;
                case ParamType.Boolean:
                    t = SqliteType.Integer;
                    break;
                case ParamType.DateTime:
                    t = SqliteType.Integer;
                    break;
                default:
                    throw new Exception("Param type is not supported");
            }

            return t;
        }

        private string getTypeSQLiteString(ParamType type)
        {
            SqliteType t;
            string result;

            t = this.getTypeSQLite(type);

            if (t == SqliteType.Integer)
            {
                result = "INTEGER";
            }
            else if (t == SqliteType.Real)
            {
                result = "REAL";
            }
            else if (t == SqliteType.Text)
            {
                result = "TEXT";
            }
            else
            {
                result = "BLOB";
            }

            return result;
        }

        private void addParameterSQLite(string name, ParamType type, object value)
        {
            SqliteType t;
            switch (type)
            {
                case ParamType.Decimal:
                    t = SqliteType.Real;
                    break;
                case ParamType.String:
                    t = SqliteType.Text;
                    break;
                case ParamType.Int16:
                    t = SqliteType.Integer;
                    break;
                case ParamType.Int32:
                    t = SqliteType.Integer;
                    break;
                case ParamType.Int64:
                    t = SqliteType.Integer;
                    break;
                case ParamType.Boolean:
                    t = SqliteType.Integer;

                    if ((bool)value == true)
                    {
                        value = 1;
                    }
                    else
                    {
                        value = 0;
                    }

                    break;
                case ParamType.DateTime:
                    t = SqliteType.Integer;
                    value = ((DateTime)value).Ticks;
                    break;
                default:
                    throw new Exception("Param type is not supported");
            }

            SqliteParameter p = new SqliteParameter(name, t);
            p.Value = value;
            this._parameters.Add(p);
        }

        public SQLStatement addParameter(string name, ParamType type, object value)
        {
            if (this.sgbd == DBMSType.MySQL)
            {
                //this.addParameterMySQL(name, type, value);
                this._connector.addParameter(this._parameters, name, type, value);
            }
            else if (this.sgbd == DBMSType.SQLite)
            {
                this.addParameterSQLite(name, type, value);
            }

            return this;
        }

        public SQLStatement addParameter(StatementParameter param)
        {
            this.addParameter(param.Nombre, param.Tipo, param.Valor);
            return this;
        }

        public SQLStatement addParameters(IList<StatementParameter> parameters)
        {
            foreach (StatementParameter item in parameters)
            {
                this.addParameter(item);
            }

            return this;
        }

        #endregion

        #region Miembros de IDisposable

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                {
                    if (this._transaction != null) this.refuseTransaction();
                    this.close();
                }
            }
            this.disposed = true;
        }

        ~SQLStatement()
        {
            this.Dispose(false);
        }

        #endregion

        /// <summary>
        /// Método que devuelve la hora del servidor
        /// </summary>
        /// <returns></returns>
        public DateTime getNow()
        {
            DateTime now;
            string sql;

            this.Semaphore.Wait();
            try
            {
                this.clearParameters();

                if (this.Type == DBMSType.MySQL)
                {
                    sql = "SELECT NOW() AS ahora;";
                }
                else
                {
                    throw new Exception("El sistema gestor de base de datos no pede proporcionar la hora del servidor");
                }

                SQLData d = this.execute(sql);
                try
                {
                    d.next();
                    now = d.getDateTime("ahora");
                }
                finally
                {
                    d.Dispose();
                }
            }
            finally
            {
                this.Semaphore.Release();
            }

            return now;
        }

        private ConfigStatement getConfigDelete(object registry)
        {
            ConfigStatement c;
            List<string> fields;
            TableAttributte table_att;
            FieldAttribute att;
            object value;
            string aux;

            c = new ConfigStatement() { SQL = "", Params = new List<StatementParameter>() };
            fields = new List<string>();

            //Get table attribute from class
            table_att = registry.GetType().GetTypeInfo().GetCustomAttribute<TableAttributte>();

            //Get fields attributes from properties
            foreach (PropertyInfo item in registry.GetType().GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
            {
                //Create SQL parameters
                att = item.GetCustomAttribute<FieldAttribute>();

                if (att != null)
                {
                    if (att.IsPrimaryKey)
                    {
                        value = item.GetValue(registry);

                        if (att.AllowNull)
                        {
                            if (att.isInteger)
                            {
                                if (att.NullValueForInt == (int)value) //always will be NOT NULL because is a integer variable
                                {
                                    value = null;
                                }
                            }
                        }

                        //c.Params.Add(new StatementParameter("@arg" + fields.Count, att.Type, item.GetValue(registry)));
                        c.Params.Add(new StatementParameter("@arg" + fields.Count, att.Type, value));
                        fields.Add(table_att.Name + "." + att.FieldName);

                        if (!att.AllowNull)
                        {
                            if (value == null)
                            {
                                throw new Exception("Field '" + att.FieldName + "' not support NULL value");
                            }
                        }
                    }
                }
            }

            //Build a INSERT statement
            c.SQL = "DELETE FROM {0} WHERE 1 = 1 {1}";

            aux = "";
            for (int i = 0; i < fields.Count; i++)
            {
                aux += " AND " + fields[i] + " = " + c.Params[i].Nombre;
            }

            if (string.IsNullOrEmpty(aux))
            {
                throw new Exception("This query can not be performed without primary keys");
            }

            c.SQL = string.Format(c.SQL, table_att.Name, aux);

            return c;
        }

        private ConfigStatement getConfigInsert(object registry)
        {
            ConfigStatement c;
            List<string> fields;
            TableAttributte table_att;
            FieldAttribute att;
            string aux;
            string aux2;
            bool autoincrement_detected;
            object value;

            autoincrement_detected = false;

            c = new ConfigStatement() { SQL = "", Params = new List<StatementParameter>() };
            fields = new List<string>();

            //Get table attribute from class
            table_att = registry.GetType().GetTypeInfo().GetCustomAttribute<TableAttributte>();

            //Get fields attributes from properties
            foreach (PropertyInfo item in registry.GetType().GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
            {
                //Create SQL parameters
                att = item.GetCustomAttribute<FieldAttribute>();

                if (att != null)
                {
                    value = item.GetValue(registry);

                    if (value == null && !att.AllowNull)
                    {
                        value = att.DefaultValue;
                    }

                    if (!att.IsAutoincrement)
                    {
                        //Check if value por int represent NULL value
                        if (att.AllowNull)
                        {
                            if (att.isInteger)
                            {
                                if (att.NullValueForInt == (int)value) //always will be NOT NULL because is a integer variable
                                {
                                    value = null;
                                }
                            }
                        }

                        //Set value
                        c.Params.Add(new StatementParameter("@arg" + fields.Count, att.Type, value));
                        fields.Add(att.FieldName);
                    }
                    else
                    {
                        if (!autoincrement_detected)
                        {
                            if (att.isInteger)
                            {
                                c.isFirstPKAutoIncrementInt = true;
                                autoincrement_detected = true;
                            }
                        }
                    }

                    if (!att.AllowNull)
                    {
                        if (value == null)
                        {
                            throw new Exception("Field '" + att.FieldName + "' not support NULL value");
                        }
                    }
                }
            }

            //Build a INSERT statement
            c.SQL = "INSERT INTO {0}({1}) VALUES ({2})";

            aux = "";
            for (int i = 0; i < fields.Count; i++)
            {
                aux += fields[i];

                if (i < fields.Count - 1)
                {
                    aux += ", ";
                }
            }

            aux2 = "";
            for (int i = 0; i < c.Params.Count; i++)
            {
                aux2 += c.Params[i].Nombre;

                if (i < c.Params.Count - 1)
                {
                    aux2 += ", ";
                }
            }

            c.SQL = string.Format(c.SQL, table_att.Name, aux, aux2);

            return c;
        }

        private ConfigStatement getConfigSelect(object registry, bool withoutwhere = false)
        {
            ConfigStatement c;
            List<string> fields;
            List<string> fields_pk;
            TableAttributte table_att;
            FieldAttribute att;
            object value;
            string aux;
            string aux2;

            c = new ConfigStatement() { SQL = "", Params = new List<StatementParameter>() };
            fields = new List<string>();
            fields_pk = new List<string>();

            //Get table attribute from class
            table_att = registry.GetType().GetTypeInfo().GetCustomAttribute<TableAttributte>();

            //Get fields attributes from properties
            foreach (PropertyInfo item in registry.GetType().GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
            {
                //Create SQL parameters
                att = item.GetCustomAttribute<FieldAttribute>();

                if (att != null)
                {
                    if (!withoutwhere)
                    {
                        if (att.IsPrimaryKey)
                        {
                            value = item.GetValue(registry);

                            if (att.isInteger)
                            {
                                if (att.NullValueForInt == (int)value) //always will be NOT NULL because is a integer variable
                                {
                                    value = null;
                                }
                            }

                            //c.Params.Add(new StatementParameter("@pk" + fields.Count, att.Type, item.GetValue(registry)));
                            c.Params.Add(new StatementParameter("@pk" + fields.Count, att.Type, value));
                            fields_pk.Add(table_att.Name + "." + att.FieldName);

                            if (!att.AllowNull)
                            {
                                if (value == null)
                                {
                                    throw new Exception("Field '" + att.FieldName + "' not support NULL value");
                                }
                            }
                        }
                    }

                    fields.Add(table_att.Name + "." + att.FieldName);
                }
            }


            //Build a INSERT statement
            c.SQL = "SELECT {1} FROM {0} WHERE 1 = 1 {2}";

            aux = "";
            for (int i = 0; i < fields.Count; i++)
            {
                aux += fields[i];

                if (i < fields.Count - 1)
                {
                    aux += ", ";
                }
            }

            aux2 = "";
            for (int i = 0; i < fields_pk.Count; i++)
            {
                aux2 += " AND " + fields_pk[i] + " = " + c.Params[i].Nombre;
            }

            if (!withoutwhere)
            {
                if (string.IsNullOrEmpty(aux2))
                {
                    throw new Exception("This query can not be performed without primary keys");
                }
            }

            c.SELECT = string.Format(c.SQL, table_att.Name, aux, "");
            c.SQL = string.Format(c.SQL, table_att.Name, aux, aux2);

            return c;
        }

        private ConfigStatement getConfigUpdate(object registry)
        {
            ConfigStatement c;
            List<StatementParameter> parameters;
            List<StatementParameter> parameters_pk;
            List<string> fields;
            List<string> fields_pk;
            TableAttributte table_att;
            FieldAttribute att;
            string aux;
            string aux2;
            object value;

            c = new ConfigStatement() { SQL = "", Params = new List<StatementParameter>() };
            fields = new List<string>();
            fields_pk = new List<string>();
            parameters = new List<StatementParameter>();
            parameters_pk = new List<StatementParameter>();

            //Get table attribute from class
            table_att = registry.GetType().GetTypeInfo().GetCustomAttribute<TableAttributte>();

            //Get fields in SET section from properties
            foreach (PropertyInfo item in registry.GetType().GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
            {
                //Create SQL parameters
                att = item.GetCustomAttribute<FieldAttribute>();

                if (att != null)
                {
                    value = item.GetValue(registry);

                    if (value == null && !att.AllowNull)
                    {
                        value = att.DefaultValue;
                    }

                    //Check if value por int represent NULL value
                    if (att.AllowNull)
                    {
                        if (att.isInteger)
                        {
                            if (att.NullValueForInt == (int)value) //always will be NOT NULL because is a integer variable
                            {
                                value = null;
                            }
                        }
                    }

                    if (!att.IsPrimaryKey)
                    {
                        parameters.Add(new StatementParameter("@arg" + fields.Count, att.Type, value));
                        fields.Add(table_att.Name + "." + att.FieldName);
                    }

                    if (!att.AllowNull)
                    {
                        if (value == null)
                        {
                            throw new Exception("Field '" + att.FieldName + "' not support NULL value");
                        }
                    }
                }
            }

            //Get fields in WHERE section from properties
            foreach (PropertyInfo item in registry.GetType().GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
            {
                //Create SQL parameters
                att = item.GetCustomAttribute<FieldAttribute>();

                if (att != null)
                {
                    if (att.IsPrimaryKey)
                    {
                        value = item.GetValue(registry);

                        //Check if value por int represent NULL value
                        if (att.AllowNull)
                        {
                            if (att.isInteger)
                            {
                                if (att.NullValueForInt == (int)value) //always will be NOT NULL because is a integer variable
                                {
                                    value = null;
                                }
                            }
                        }

                        parameters_pk.Add(new StatementParameter("@pk" + fields_pk.Count, att.Type, value));
                        fields_pk.Add(table_att.Name + "." + att.FieldName);

                        if (!att.AllowNull)
                        {
                            if (value == null)
                            {
                                throw new Exception("Field '" + att.FieldName + "' not support NULL value");
                            }
                        }
                    }
                }
            }


            c.SQL = "UPDATE {0} SET {1} WHERE 1 = 1 {2}";

            //Build SET section in UPDATE statement
            aux = "";
            for (int i = 0; i < fields.Count; i++)
            {
                aux += fields[i] + " = " + parameters[i].Nombre;

                if (i < fields.Count - 1)
                {
                    aux += ", ";
                }

                c.Params.Add(parameters[i]);
            }

            //Build WHERE section in UPDATE statement
            aux2 = "";
            for (int i = 0; i < fields_pk.Count; i++)
            {
                aux2 += " AND " + fields_pk[i] + " = " + parameters_pk[i].Nombre;

                c.Params.Add(parameters_pk[i]);
            }

            if (string.IsNullOrEmpty(aux2))
            {
                throw new Exception("This query can not be performed without primary keys");
            }

            c.SQL = string.Format(c.SQL, table_att.Name, aux, aux2);

            return c;
        }

        private void updateFirstAutoincrement(object registry, int value)
        {
            FieldAttribute att;

            foreach (PropertyInfo item in registry.GetType().GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
            {
                att = item.GetCustomAttribute<FieldAttribute>();

                if (att != null)
                {
                    if (att.IsAutoincrement && (att.Type == ParamType.Int16 || att.Type == ParamType.Int32 || att.Type == ParamType.Int64))
                    {
                        item.SetValue(registry, value);

                        break;
                    }
                }
            }
        }

        public async Task<bool> emptyTableAsync<T>() where T : class, new()
        {
            T t;
            TableAttributte table_att;
            string sql;
            bool result;

            t = new T();

            //Get table attribute from class
            table_att = t.GetType().GetTypeInfo().GetCustomAttribute<TableAttributte>();
            sql = "DELETE FROM {0} WHERE 1 = 1";
            sql = string.Format(sql, table_att.Name);

            await this.Semaphore.WaitAsync();
            try
            {
                result = await this.executeNonQueryAsync(sql) > 0;
            }
            finally
            {
                this.Semaphore.Release();
            }

            return result;
        }

        public bool emptyTable<T>() where T : class, new()
        {
            return this.emptyTableAsync<T>().Result;
        }

        public bool load<T>(T registry) where T : class, new()
        {
            return this.loadAsync<T>(registry).Result;
        }

        public async Task<bool> loadAsync<T>(T registry) where T : class, new()
        {
            ConfigStatement c;
            bool result;

            await this.Semaphore.WaitAsync();
            try
            {
                c = this.getConfigSelect(registry);

                this.addParameters(c.Params);

                using (SQLData d = await this.executeAsync(c.SQL))
                {
                    if (d.next())
                    {
                        d.fill<T>(registry);
                        result = true;
                    }
                    else
                    {
                        result = false;
                    }
                }
            }
            finally
            {
                this.Semaphore.Release();
            }

            return result;
        }

        public List<T> select<T>(string sql = "", string orderby = "", List<StatementParameter> parameters = null) where T : class, new()
        {
            return this.selectAsync<T>(sql, orderby, parameters).Result;
        }

        public async Task<List<T>> selectAsync<T>(string sql = "", string orderby = "", List<StatementParameter> parameters = null) where T : class, new()
        {
            ConfigStatement c;
            List<T> result;

            if (sql == null)
            {
                sql = "";
            }

            await this.Semaphore.WaitAsync();
            try
            {
                c = this.getConfigSelect(new T(), true);

                if (parameters != null)
                {
                    this.addParameters(parameters);
                }

                using (SQLData d = await this.executeAsync(c.SELECT + " " + sql))
                {
                    result = d.fillToList<T>();
                }
            }
            finally
            {
                this.Semaphore.Release();
            }

            return result;
        }

        public bool insert(object registry)
        {
            return this.insertAsync(registry).Result;
        }

        public async Task<bool> insertAsync(object registry)
        {
            ConfigStatement c;
            bool result;

            await this.Semaphore.WaitAsync();
            try
            {
                c = this.getConfigInsert(registry);

                this.addParameters(c.Params);
                result = await this.executeNonQueryAsync(c.SQL) > 0;

                if (result)
                {
                    this.updateFirstAutoincrement(registry, this.lastID);
                }
            }
            finally
            {
                this.Semaphore.Release();
            }

            return result;
        }

        public bool update(object registry)
        {
            return this.updateAsync(registry).Result;
        }

        public async Task<bool> updateAsync(object registry)
        {
            ConfigStatement c;
            bool result;
            int records;

            await this.Semaphore.WaitAsync();
            try
            {
                c = this.getConfigUpdate(registry);

                this.addParameters(c.Params);
                records = await this.executeNonQueryAsync(c.SQL);
            }
            finally
            {
                this.Semaphore.Release();
            }

            result = records > 0;

            return result;
        }

        public bool delete(object registry)
        {
            return this.deleteAsync(registry).Result;
        }

        public async Task<bool> deleteAsync(object registry)
        {
            ConfigStatement c;
            bool result;

            await this.Semaphore.WaitAsync();
            try
            {
                c = this.getConfigDelete(registry);

                this.addParameters(c.Params);
                result = await this.executeNonQueryAsync(c.SQL) > 0;
            }
            finally
            {
                this.Semaphore.Release();
            }

            return result;
        }

        private string getCreateFieldMySQL(FieldAttribute field_info, bool without_pk = false)
        {
            string result;

            result = field_info.FieldName + " " + this._connector.getTypeString(field_info.Type, field_info.Size);

            if (field_info.IsAutoincrement && (field_info.Type == ParamType.Int16 || field_info.Type == ParamType.Int32 || field_info.Type == ParamType.Int64))
            {
                result += " AUTO_INCREMENT";
            }

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

        private string getCreateFieldSQLite(FieldAttribute field_info, bool without_pk = false)
        {
            string result;

            result = field_info.FieldName + " " + this.getTypeSQLiteString(field_info.Type);

            if (field_info.IsPrimaryKey && !without_pk)
            {
                result += " PRIMARY KEY";
            }

            if (field_info.IsAutoincrement && (field_info.Type == ParamType.Int16 || field_info.Type == ParamType.Int32 || field_info.Type == ParamType.Int64))
            {
                result += " AUTOINCREMENT";
            }

            if (!field_info.IsPrimaryKey || (field_info.IsPrimaryKey && without_pk))
            {
                if (!field_info.AllowNull)
                {
                    result += " NOT NULL";
                }
                //else
                //{
                //    result += " NULL";
                //}
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

        private async Task<bool> createUpdateTableMySQLAsync<T>() where T : class, new()
        {
            T t;
            TableAttributte table_att;
            FieldAttribute field_att;
            List<FieldAttribute> pks;
            List<string> pks_at_table;
            SQLData ds;
            bool result;
            bool new_table;
            string sql;

            result = true;

            t = new T();
            pks = new List<FieldAttribute>();

            //check if table exists
            table_att = t.GetType().GetTypeInfo().GetCustomAttribute<TableAttributte>();

            try
            {
                sql = "SELECT * FROM " + table_att.Name + " LIMIT 1";
                await this.executeAsync(sql);
                result = true;
                new_table = false;
            }
            catch
            {
                result = false;
                new_table = true;
            }

            //if not exists create a table empty
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

                await this.executeNonQueryAsync(sql);
            }

            //Obtain primaries keys from table
            pks_at_table = new List<string>();
            sql = "SHOW KEYS FROM " + table_att.Name + " WHERE Key_name = 'PRIMARY'";
            ds = await this.executeAsync(sql);
            while (ds.next())
            {
                pks_at_table.Add(ds.getString("Column_name"));
            }

            //Obtain new primary keys from data structure
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

            //Compare pk from target in source
            if (pks.Count == pks_at_table.Count)
            {

            }

            //if (pks.Count == 0)
            //{
            //    sql += "";
            //}
            //else
            //{
            //    for (int i = 0; i < pks.Count; i++)
            //    {
            //        sql += this.getCreateFieldMySQL(pks[i]);

            //        if (i < pks.Count - 1)
            //        {
            //            sql += ", ";
            //        }
            //    }
            //}
            //sql += ")";

            return result;
        }

        private async Task<bool> createUpdateTableSQLiteAsync<T>() where T : class, new()
        {
            T t;
            TableAttributte table_att;
            FieldAttribute field_att;
            List<FieldAttribute> pks;
            bool result;
            bool new_table;
            string sql;

            result = true;

            t = new T();
            pks = new List<FieldAttribute>();

            //Check if table exists
            table_att = t.GetType().GetTypeInfo().GetCustomAttribute<TableAttributte>();

            try
            {
                sql = "SELECT * FROM " + table_att.Name + " LIMIT 1";
                await this.executeAsync(sql);
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
                        sql += this.getCreateFieldSQLite(pks[i]);

                        if (i < pks.Count - 1)
                        {
                            sql += ", ";
                        }
                    }
                }
                sql += ")";

                await this.executeNonQueryAsync(sql);
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
                            await this.executeAsync(sql);
                            result = true;
                        }
                        catch
                        {
                            result = false;
                        }

                        //Create field
                        if (!result)
                        {
                            sql = "ALTER TABLE " + table_att.Name + " ADD COLUMN " + this.getCreateFieldSQLite(field_att, true);
                            await this.executeNonQueryAsync(sql);
                            result = true;
                        }
                    }
                }
            }

            return result;
        }

        public async Task<bool> createUpdateTableAsync<T>() where T : class, new()
        {
            bool result;

            if (this.sgbd == DBMSType.SQLite)
            {
                result = await this.createUpdateTableSQLiteAsync<T>();
            }
            if (this.sgbd == DBMSType.MySQL)
            {
                result = await this.createUpdateTableMySQLAsync<T>();
            }
            else
            {
                result = false;
            }

            return result;
        }

        public bool createUpdateTable<T>() where T : class, new()
        {
            return this.createUpdateTableAsync<T>().Result;
        }
    }
}
