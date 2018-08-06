using System;
using System.Collections.Generic;
using System.Text;

namespace es.dmoreno.utils.dataaccess.db
{
    public class ConnectionParameters
    {
        public DBMSType Type { get; set; } = DBMSType.None;

        public string Host { get; set; }
        public string Database { get; set; }
        public string User { get; set; }
        public string Password { get; set; }
        public int Port { get; set; }

        public string File { get; set; }

        public IConnector Connector { get; set; }

        public bool BeginTransaction { get; set; } = false;
    }
}
