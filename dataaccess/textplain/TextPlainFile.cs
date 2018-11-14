using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace es.dmoreno.utils.dataaccess.textplain
{
    public class TextPlainFile : IDisposable
    {
        private string _filename;
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

            result = (this._fs = new FileStream(this._filename, FileMode.OpenOrCreate)) != null;

            return result;
        }

        public string get()
        {
            string txt;
            string line;

            if (this._fs == null)
            {
                throw new IOException("File " + this._filename + " is not open");
            }

            this._fs.Position = 0;

            using (StreamReader sr = new StreamReader(this._fs))
            {
                txt = "";

                while ((line = sr.ReadLine()) != null)
                {
                    txt += line;
                }
            }

            return txt;
        }

        public void set(string text)
        {
            if (this._fs == null)
            {
                throw new IOException("File " + this._filename + " is not open");
            }

            using (StreamWriter sw = new StreamWriter(this._fs))
            {
                sw.Write(text);
            }
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
                    if (this._fs != null)
                    {
                        this._fs.Dispose();
                        this._fs = null;
                    }
                }

                // TODO: libere los recursos no administrados (objetos no administrados) y reemplace el siguiente finalizador.
                // TODO: configure los campos grandes en nulos.
                this._filename = null;

                disposedValue = true;
            }
        }

        // TODO: reemplace un finalizador solo si el anterior Dispose(bool disposing) tiene código para liberar los recursos no administrados.
        // ~TextPlainFile() {
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
