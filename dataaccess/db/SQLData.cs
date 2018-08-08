using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Reflection;
using System.Text;

namespace es.dmoreno.utils.dataaccess.db
{
    public class SQLData : IDisposable
    {
        internal object Semaphore { get; } = new object();

        private SQLStatement _statement;
        private DbDataReader _dr;
        private DbCommand _command;
        private bool _disposed = false;

        internal SQLData(DbDataReader dr, DbCommand cmd, SQLStatement statement)
        {
            this._dr = dr;
            this._command = cmd;
            this._statement = statement;
        }

        private void checkDisposed()
        {
            if (this._disposed)
                throw new Exception("Se intenta leer de un elemento Datos finalizado");
        }

        public bool next()
        {
            this.checkDisposed();
            return this._dr.Read();
        }

        public bool isEmpty()
        {
            this.checkDisposed();
            return !this._dr.HasRows;
        }

        public bool isNull(string field)
        {
            this.checkDisposed();
            if (string.IsNullOrEmpty(field))
                throw new Exception("No se puede saber si un campo sin nombre es nulo");

            return this._dr.IsDBNull(this._dr.GetOrdinal(field));
        }

        public object getObject(string field)
        {
            this.checkDisposed();
            if (string.IsNullOrEmpty(field))
                throw new Exception("No se puede saber el contenido de un campo sin nombre");

            return this._dr.GetValue(this._dr.GetOrdinal(field));
        }

        public bool getBool(string field)
        {
            bool value;

            this.checkDisposed();
            if (string.IsNullOrEmpty(field))
                throw new Exception("No se puede saber el contenido de un campo sin nombre");

            if (this._statement.Type == DBMSType.SQLite)
            {
                value = this._dr.GetInt32(this._dr.GetOrdinal(field)) == 1;
            }
            else
            {
                value = this._dr.GetBoolean(this._dr.GetOrdinal(field));
            }

            return value;
        }

        public Int16 getInt16(string field)
        {
            this.checkDisposed();
            if (string.IsNullOrEmpty(field))
                throw new Exception("No se puede saber el contenido de un campo sin nombre");

            return this._dr.GetInt16(this._dr.GetOrdinal(field));
        }

        public Int32 getInt32(string field)
        {
            this.checkDisposed();
            if (string.IsNullOrEmpty(field))
                throw new Exception("No se puede saber el contenido de un campo sin nombre");

            return this._dr.GetInt32(this._dr.GetOrdinal(field));
        }

        public Int64 getInt64(string field)
        {
            this.checkDisposed();
            if (string.IsNullOrEmpty(field))
                throw new Exception("No se puede saber el contenido de un campo sin nombre");

            return this._dr.GetInt64(this._dr.GetOrdinal(field));
        }

        public string getString(string field)
        {
            this.checkDisposed();
            if (string.IsNullOrEmpty(field))
                throw new Exception("No se puede saber el contenido de un campo sin nombre");

            return this._dr.GetString(this._dr.GetOrdinal(field));
        }

        public DateTime getDateTime(string field)
        {
            DateTime value;

            this.checkDisposed();
            if (string.IsNullOrEmpty(field))
                throw new Exception("No se puede saber el contenido de un campo sin nombre");

            if (this._statement.Type == DBMSType.SQLite)
            {
                value = new DateTime(this._dr.GetInt64(this._dr.GetOrdinal(field)));
            }
            else
            {
                value = this._dr.GetDateTime(this._dr.GetOrdinal(field));
            }

            return value;
        }

        public decimal getDecimal(string field)
        {
            this.checkDisposed();
            if (string.IsNullOrEmpty(field))
                throw new Exception("No se puede saber el contenido de un campo sin nombre");

            return this._dr.GetDecimal(this._dr.GetOrdinal(field));
        }

        /// <summary>
        /// Obtiene el contenido del campo comprobando si es nulo pero no genera una excepción en caso positivo
        /// </summary>
        /// <param name="field"></param>
        /// <returns>IMPORTANTE: Devolverá false si es campo nulo si no el contenido del campo</returns>
        public bool getBoolWithoutExcept(string field)
        {
            if (this.isNull(field))
                return false;
            else
                return this.getBool(field);
        }

        /// <summary>
        /// Obtiene el contenido del campo comprobando si es nulo pero no genera una excepción en caso positivo
        /// </summary>
        /// <param name="field"></param>
        /// <returns>IMPORTANTE: Devolverá 0 si es campo nulo si no el contenido del campo</returns>
        public Int16 getInt16WithoutExcept(string field)
        {
            if (this.isNull(field))
                return 0;
            else
                return this.getInt16(field);
        }

        /// <summary>
        /// Obtiene el contenido del campo comprobando si es nulo pero no genera una excepción en caso positivo
        /// </summary>
        /// <param name="field"></param>
        /// <returns>IMPORTANTE: Devolverá 0 si es campo nulo si no el contenido del campo</returns>
        public Int32 getInt32WithoutExcept(string field)
        {
            if (this.isNull(field))
                return 0;
            else
                return this.getInt32(field);
        }

        /// <summary>
        /// Obtiene el contenido del campo comprobando si es nulo pero no genera una excepción en caso positivo
        /// </summary>
        /// <param name="field"></param>
        /// <returns>IMPORTANTE: Devolverá 0 si es campo nulo si no el contenido del campo</returns>
        public Int64 getInt64WithoutExcept(string field)
        {
            if (this.isNull(field))
                return 0;
            else
                return this.getInt64(field);
        }

        /// <summary>
        /// Obtiene el contenido del campo comprobando si es nulo pero no genera una excepción en caso positivo
        /// </summary>
        /// <param name="field"></param>
        /// <returns>IMPORTANTE: Devolverá cadena vacia si es campo nulo si no el contenido del campo</returns>
        public string getStringWithoutExcept(string field)
        {
            if (this.isNull(field))
                return "";
            else
                return this.getString(field);
        }

        /// <summary>
        /// Obtiene el contenido del campo comprobando si es nulo pero no genera una excepción en caso positivo
        /// </summary>
        /// <param name="field"></param>
        /// <returns>IMPORTANTE: Devolverá '1970-01-01 0:0:0' si es campo nulo si no el contenido del campo</returns>
        public DateTime getDateTimeWithoutExcept(string field)
        {
            if (this.isNull(field))
                return new DateTime(0);
            else
                return this.getDateTime(field);
        }

        /// <summary>
        /// Obtiene el contenido del campo comprobando si es nulo pero no genera una excepción en caso positivo
        /// </summary>
        /// <param name="field"></param>
        /// <returns>IMPORTANTE: Devolverá 0 si es campo nulo si no el contenido del campo</returns>
        public decimal getDecimalWithoutExcept(string field)
        {
            if (this.isNull(field))
                return 0;
            else
                return this.getDecimal(field);
        }

        

        private T fill<T>(List<PropertyInfo> p, List<FieldAttribute> a = null) where T : class, new()
        {
            var registry = new T();

            List<FieldAttribute> attributes;
            if (a == null)
            {
                attributes = Utils.getFieldAttributes(p);
            }
            else
            {
                attributes = a;
            }

            for (int i = 0; i < p.Count; i++)
            {
                var item = p[i];
                var att = attributes[i];

                if (att.Type == ParamType.Boolean)
                {
                    if (att.AllowNull)
                    {
                        if (this.isNull(att.FieldName))
                        {
                            item.SetValue(registry, false);
                        }
                        else
                        {
                            item.SetValue(registry, this.getBool(att.FieldName));
                        }
                    }
                    else
                    {
                        item.SetValue(registry, this.getBool(att.FieldName));
                    }
                }
                else if (att.Type == ParamType.DateTime)
                {
                    if (att.AllowNull)
                    {
                        if (this.isNull(att.FieldName))
                        {
                            item.SetValue(registry, null);
                        }
                        else
                        {
                            item.SetValue(registry, this.getDateTime(att.FieldName));
                        }
                    }
                    else
                    {
                        item.SetValue(registry, this.getDateTime(att.FieldName));
                    }
                }
                else if (att.Type == ParamType.ByteArray)
                {
                    throw new Exception("Parameter type ByteArray is not supported");
                }
                else if (att.Type == ParamType.Decimal)
                {
                    if (att.AllowNull)
                    {
                        if (this.isNull(att.FieldName))
                        {
                            item.SetValue(registry, 0);
                        }
                        else
                        {
                            item.SetValue(registry, this.getDecimal(att.FieldName));
                        }
                    }
                    else
                    {
                        item.SetValue(registry, this.getDecimal(att.FieldName));
                    }
                }
                else if (att.Type == ParamType.Int16)
                {
                    if (att.AllowNull)
                    {
                        if (this.isNull(att.FieldName))
                        {
                            item.SetValue(registry, att.NullValueForInt);
                        }
                        else
                        {
                            item.SetValue(registry, this.getInt16(att.FieldName));
                        }
                    }
                    else
                    {
                        item.SetValue(registry, this.getInt16(att.FieldName));
                    }
                }
                else if (att.Type == ParamType.Int32)
                {
                    if (att.AllowNull)
                    {
                        if (this.isNull(att.FieldName))
                        {
                            item.SetValue(registry, att.NullValueForInt);
                        }
                        else
                        {
                            item.SetValue(registry, this.getInt32(att.FieldName));
                        }
                    }
                    else
                    {
                        item.SetValue(registry, this.getInt32(att.FieldName));
                    }
                }
                else if (att.Type == ParamType.Int64)
                {
                    if (att.AllowNull)
                    {
                        if (this.isNull(att.FieldName))
                        {
                            item.SetValue(registry, att.NullValueForInt);
                        }
                        else
                        {
                            item.SetValue(registry, this.getInt32(att.FieldName));
                        }
                    }
                    else
                    {
                        item.SetValue(registry, this.getInt32(att.FieldName));
                    }
                }
                else if (att.Type == ParamType.String || att.Type == ParamType.LongString)
                {
                    if (att.AllowNull)
                    {
                        if (this.isNull(att.FieldName))
                        {
                            item.SetValue(registry, null);
                        }
                        else
                        {
                            item.SetValue(registry, this.getString(att.FieldName));
                        }
                    }
                    else
                    {
                        item.SetValue(registry, this.getString(att.FieldName));
                    }
                }
            }

            return registry;
        }

        public T fill<T>(T reg = null) where T : class, new()
        {
            if (reg == null)
            {
                reg = new T();
            }

            var properties = Utils.getPropertyInfos<T>(reg, true);
            var attributes = Utils.getFieldAttributes(properties);

            return this.fill<T>(properties, attributes);
        }

        public List<T> fillToList<T>() where T : class, new()
        {
            List<T> list;

            var properties = Utils.getPropertyInfos<T>(new T(), true);
            var attributes = Utils.getFieldAttributes(properties);

            if (this.next())
            {
                list = new List<T>();

                do
                {
                    list.Add(this.fill<T>(properties));
                }
                while (this.next());
            }
            else
            {
                list = null;
            }

            return list;
        }

        #region Miembros de IDisposable
        public void close()
        {
            if (this._dr != null)
            {
                this._dr.Dispose();
                this._dr = null;
            }

            if (this._command != null)
            {
                this._command.Dispose();
                this._command = null;
            }
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
                    this.close();
                }
            }
            this._disposed = true;
        }

        ~SQLData()
        {
            this.Dispose(false);
        }

        #endregion
    }
}
