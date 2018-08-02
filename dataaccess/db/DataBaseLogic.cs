using System;
using System.Collections.Generic;
using System.Text;

namespace es.dmoreno.utils.dataaccess.db
{
    public class DataBaseLogic : IDisposable
    {
        static private string createStringConnection(DBMSType type, string host, string database, string user, string password, int port)
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

        private ConnectionParameters _parameters;
        private SQLStatement _connection;
        private Management _management;
        private IConnector _connector;
        private DBMSType _type;
        private string _string_connection;
        private bool _create_with_begin_transaction;
              
        public SQLStatement Statement
        {
            get
            {
                if (this._connection == null)
                {
                    this._connection = new SQLStatement(this._string_connection, this._type, this._connector);

                    if (this._create_with_begin_transaction)
                    {
                        this._connection.beginTransaction();
                    }
                }

                return this._connection;
            }
        }

        public Management Management
        {
            get
            {
                if (this._management == null)
                {
                    if (this._type == DBMSType.SQLite)
                    {
                        this._management = new SQLiteManagement(this.Statement);
                    }
                    else
                    {
                        throw new NotImplementedException();
                    }
                }

                return this._management;
            }
        }

        public DataBaseLogic(ConnectionParameters p)
        {
            this.initilize(p);
        }

        private void initilize(ConnectionParameters p)
        {
            this._connection = null;
            this._connector = p.Connector; ;
            this._type = p.Type;
            this._string_connection = DataBaseLogic.createStringConnection(p.Type, p.Host, p.Database, p.User, p.Password, p.Port);
            this._parameters = p;
            this._create_with_begin_transaction = p.BeginTransaction;
        }

        public DataBaseLogic duplicate()
        {
            return new DataBaseLogic(this._parameters);
        }

        #region IDisposable Support
        private bool disposedValue = false; // Para detectar llamadas redundantes

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: elimine el estado administrado (objetos administrados).
                    if (this._connection != null)
                    {
                        this._connection.Dispose();
                    }
                }

                // TODO: libere los recursos no administrados (objetos no administrados) y reemplace el siguiente finalizador.
                // TODO: configure los campos grandes en nulos.

                disposedValue = true;
            }
        }

        // TODO: reemplace un finalizador solo si el anterior Dispose(bool disposing) tiene código para liberar los recursos no administrados.
        // ~DataBaseLogic() {
        //   // No cambie este código. Coloque el código de limpieza en el anterior Dispose(colocación de bool).
        //   Dispose(false);
        // }

        // Este código se agrega para implementar correctamente el patrón descartable.
        public void Dispose()
        {
            // No cambie este código. Coloque el código de limpieza en el anterior Dispose(colocación de bool).
            Dispose(true);
            // TODO: quite la marca de comentario de la siguiente línea si el finalizador se ha reemplazado antes.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}
