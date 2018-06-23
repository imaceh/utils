using System;
using System.Collections.Generic;
using System.Text;

namespace es.dmoreno.utils.dataaccess.db
{
    public class Logic : IDisposable
    {
        private SQLStatement _connection;
        private IConnector _connector;
        private DBMSType _type;
        private string _string_connection;

        private bool _disposed;

        static public string createStringConnection(DBMSType type, string host, string database, string user, string password, int port)
        {
            string string_connection;

            if (type == DBMSType.MySQL)
            {
                string_connection = string.Format("Server={0};Database={1};Uid={2};Pwd={3};Port={4};Pooling=true;Encrypt=false;", host, database, user, password, port.ToString());
            }
            else if (type == DBMSType.SQLite)
            {
                string_connection = string.Format("Data Source={0};", host);
            }
            else
            {
                string_connection = null;
            }

            return string_connection;
        }

        public SQLStatement Statement
        {
            get
            {
                if (this._connection == null)
                {
                    this._connection = new SQLStatement(this._string_connection, this._type, this._connector);
                }

                return this._connection;
            }
        }

        public string ConnectionString
        {
            get
            {
                return this._string_connection;
            }

            set
            {
                if (this._connection != null)
                {
                    this._connection.Dispose();
                }

                this._string_connection = value;
            }
        }

        public Logic(DBMSType type)
        {
            this.initilize(null, type, null, null);
        }

        public Logic(DBMSType type, string string_connection, IConnector connector)
        {
            this.initilize(null, type, string_connection, connector);
        }

        private void initilize(SQLStatement connection, DBMSType type, string string_connection, IConnector connector)
        {
            this._connection = connection;
            this._connector = connector;
            this._type = type;
            this._string_connection = string_connection;
            this._disposed = false;
        }

        public Logic duplicate()
        {
            Logic new_object;

            new_object = new Logic(this._type, this._string_connection, this._connector);

            return new_object;
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!this._disposed)
            {
                if (disposing)
                {
                    if (this._connection != null)
                    {
                        this._connection.Dispose();
                    }
                }
            }
            this._disposed = true;
        }

        ~Logic()
        {
            this.Dispose(false);
        }
    }
}
