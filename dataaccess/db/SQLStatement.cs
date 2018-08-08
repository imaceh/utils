/*
* 29/07/2018: Resolve error when use SQLite database and her state is locked (https://github.com/aspnet/Microsoft.Data.Sqlite/issues/474)
*/
using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
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

        private const int kMaxTimeWaitUnlock = 30000;

        public string LastError { get; internal set; }

        internal IConnector Conector { get { return this._connector; } }

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

        static private bool isBusySQLite(int code)
        {
            return (code == SQLitePCL.raw.SQLITE_BUSY) || (code == SQLitePCL.raw.SQLITE_LOCKED) || (code == SQLitePCL.raw.SQLITE_LOCKED_SHAREDCACHE);
        }

        public SQLData execute(string sql)
        {
            bool completed = false;
            this.finalizeSqlData();
            this.createCommand();
            this._command.CommandText = sql;

            var stopwatch = Stopwatch.StartNew();
            while (!completed)
            {
                try
                {
                    this._datareader = this._command.ExecuteReader();
                    completed = true;
                }
                catch (SqliteException ex)
                {
                    if (!isBusySQLite(ex.SqliteErrorCode) || stopwatch.ElapsedMilliseconds > kMaxTimeWaitUnlock)
                    {
                        this.LastError = ex.Message;
                        throw ex;
                    }
                }
                catch (Exception ex)
                {
                    this.LastError = ex.Message;
                    throw ex;
                }
            }
            SQLData d = new SQLData(this._datareader, this._command, this);
            this._result_data = d;
            this.empty();
            return d;
        }

        public async Task<SQLData> executeAsync(string sql)
        {
            bool completed = false;
            this.finalizeSqlData();
            this.createCommand();
            this._command.CommandText = sql;

            var stopwatch = Stopwatch.StartNew();
            while (!completed)
            {
                try
                {
                    this._datareader = await this._command.ExecuteReaderAsync();
                    completed = true;
                }
                catch (SqliteException ex)
                {
                    if (!isBusySQLite(ex.SqliteErrorCode) || stopwatch.ElapsedMilliseconds > kMaxTimeWaitUnlock)
                    {
                        this.LastError = ex.Message;
                        throw ex;
                    }
                }
                catch (Exception ex)
                {
                    this.LastError = ex.Message;
                    throw ex;
                }
            }

            SQLData d = new SQLData(this._datareader, this._command, this);
            this._result_data = d;
            this.empty();
            return d;
        }

        public int executeNonQuery(string sql)
        {
            bool completed = false;
            int resultado = 0;

            this.finalizeSqlData();
            this.createCommand();
            this._command.CommandText = sql;

            var stopwatch = Stopwatch.StartNew();
            while (!completed)
            {
                try
                {
                    resultado = this._command.ExecuteNonQuery();
                    completed = true;
                }
                catch (SqliteException ex)
                {
                    if (!isBusySQLite(ex.SqliteErrorCode) || stopwatch.ElapsedMilliseconds > kMaxTimeWaitUnlock)
                    {
                        this.LastError = ex.Message;
                        throw ex;
                    }
                }
                catch (Exception ex)
                {
                    this.LastError = ex.Message;
                    throw ex;
                }
            }

            this.empty();
            return resultado;
        }

        public async Task<int> executeNonQueryAsync(string sql)
        {
            bool completed = false;
            int resultado = 0;

            this.finalizeSqlData();
            this.createCommand();
            this._command.CommandText = sql;

            var stopwatch = Stopwatch.StartNew();
            while (!completed)
            {
                try
                {
                    resultado = await this._command.ExecuteNonQueryAsync();
                    completed = true;
                }
                catch (SqliteException ex)
                {
                    if (!isBusySQLite(ex.SqliteErrorCode) || stopwatch.ElapsedMilliseconds > kMaxTimeWaitUnlock)
                    {
                        this.LastError = ex.Message;
                        throw ex;
                    }
                }
                catch (Exception ex)
                {
                    this.LastError = ex.Message;
                    throw ex;
                }
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

        internal SqliteType getTypeSQLite(ParamType type)
        {
            SqliteType t;
            switch (type)
            {
                case ParamType.Decimal:
                    t = SqliteType.Real;
                    break;
                case ParamType.String:
                case ParamType.LongString:
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

        internal string getTypeSQLiteString(ParamType type)
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
                case ParamType.LongString:
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
            TableAttribute table_att;
            FieldAttribute att;
            object value;
            string aux;

            c = new ConfigStatement() { SQL = "", Params = new List<StatementParameter>() };
            fields = new List<string>();

            //Get table attribute from class
            table_att = registry.GetType().GetTypeInfo().GetCustomAttribute<TableAttribute>();

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
            TableAttribute table_att;
            FieldAttribute att;
            string aux;
            string aux2;
            bool autoincrement_detected;
            object value;

            autoincrement_detected = false;

            c = new ConfigStatement() { SQL = "", Params = new List<StatementParameter>() };
            fields = new List<string>();

            //Get table attribute from class
            table_att = registry.GetType().GetTypeInfo().GetCustomAttribute<TableAttribute>();

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
            TableAttribute table_att;
            FieldAttribute att;
            object value;
            string aux;
            string aux2;

            c = new ConfigStatement() { SQL = "", Params = new List<StatementParameter>() };
            fields = new List<string>();
            fields_pk = new List<string>();

            //Get table attribute from class
            table_att = registry.GetType().GetTypeInfo().GetCustomAttribute<TableAttribute>();

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
            TableAttribute table_att;
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
            table_att = registry.GetType().GetTypeInfo().GetCustomAttribute<TableAttribute>();

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
                        if (this.Type == DBMSType.SQLite)
                        {
                            fields.Add(att.FieldName);
                        }
                        else
                        {
                            fields.Add(table_att.Name + "." + att.FieldName);
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
            TableAttribute table_att;
            string sql;
            bool result;

            t = new T();

            //Get table attribute from class
            table_att = t.GetType().GetTypeInfo().GetCustomAttribute<TableAttribute>();
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

        public async Task<T> firstAsync<T>(string sql = "", string orderby = "", List<StatementParameter> parameters = null) where T : class, new()
        {
            ConfigStatement c;
            T result;

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

                using (SQLData d = await this.executeAsync(c.SELECT + " " + sql + " LIMIT 1"))
                {
                    if (d.next())
                    {
                        result = d.fill<T>();
                    }
                    else
                    {
                        result = null;
                    }
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
    }
}
