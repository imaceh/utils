using System;
using System.Collections.Generic;
using System.Text;

namespace es.dmoreno.utils.dataaccess.db
{
    public enum EngineType
    {
        /// <summary>
        /// No set Engine type in create statement
        /// </summary>
        Default = 0,

        /// <summary>
        /// Set InnoDB as engine for table in create statement
        /// </summary>
        InnoDB = 1,

        /// <summary>
        /// Set MyISAM as engine for table in create statement
        /// </summary>
        MyISAM = 2
    }
}
