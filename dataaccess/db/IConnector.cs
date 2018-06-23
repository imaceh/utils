using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace es.dmoreno.utils.dataaccess.db
{
    public interface IConnector
    {
        /// <summary>
        /// Returns an object of type DBConnection to make a connection to a database
        /// </summary>
        /// <param name="connectionString"></param>
        /// <returns></returns>
        DbConnection getConnection(string connectionString);

        /// <summary>
        /// Add parameters to perform a statement
        /// </summary>
        /// <param name="parameters"></param>
        /// <param name="name"></param>
        /// <param name="type"></param>
        /// <param name="value"></param>
        void addParameter(List<DbParameter> parameters, string name, ParamType type, object value);

        /// <summary>
        /// Perform a conversion ParamType to name of equivalent in DB syntax
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        string getTypeString(ParamType type, params int[] size);

        /// <summary>
        /// Perform a conversion ParamType to id of equivalent in DB syntax
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        int getTypeInt(ParamType type);

        /// <summary>
        /// 
        /// </summary>
        DBMSType Type { get; }
    }
}
