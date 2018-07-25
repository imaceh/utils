using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace es.dmoreno.utils.dataaccess.textplain
{
    public class TextPlainFile
    {
        private string _filename;
        private string _content;
        private FileStream _fs;

        public TextPlainFile(string filename)
        {
            this._filename = filename;
            this._fs = null;
        }

        public bool open()
        {
            bool result;

            if (this._fs != null)
            {
                this._fs.Dispose();
            }

            if (!File.Exists(this._filename))
            {
                result = false;
            }
            else
            {
                result = (this._fs = new FileStream(this._filename, FileMode.OpenOrCreate)) != null;

                if (result)
                {

                }
            }

            return result;
        }
    }
}
