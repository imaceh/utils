using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

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
            }

            return result;
        }

        public async Task<string> getAsync()
        {
            string txt;
            string line;

            if (this._fs == null)
            {
                throw new IOException("File " + this._filename + " is not open");
            }

            using (StreamReader sr = new StreamReader(this._fs))
            {
                this._fs.Position = 0;
                txt = "";

                while ((line = await sr.ReadLineAsync()) != null)
                {
                    txt += line;
                }
            }

            return txt;
        }

        public async Task set(string text)
        {
            if (this._fs == null)
            {
                throw new IOException("File " + this._filename + " is not open");
            }

            using (StreamWriter sw = new StreamWriter(this._fs))
            {
                await sw.WriteAsync(text);
            }
        }
    }
}
