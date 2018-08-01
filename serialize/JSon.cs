using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Json;
using System.Text;

namespace es.dmoreno.utils.serialize
{
    static public class JSon
    {
        static public T deserializeJSON<T>(string json) where T : class
        {
            MemoryStream ms;
            DataContractJsonSerializer ser;
            T var;

            var settings = new DataContractJsonSerializerSettings
            {
                DateTimeFormat = new System.Runtime.Serialization.DateTimeFormat("yyyy-MM-ddTHH:mm:ssK"),
            };

            using (ms = new MemoryStream(Encoding.UTF8.GetBytes(json)))
            {
                ser = new DataContractJsonSerializer(typeof(T), settings);
                var = ser.ReadObject(ms) as T;
            }

            return var;
        }

        static public string serializeJSON<T>(T obj) where T : class
        {
            MemoryStream ms;
            DataContractJsonSerializer ser;
            byte[] array;
            string result;

            using (ms = new MemoryStream())
            {
                ser = new DataContractJsonSerializer(typeof(T));
                ser.WriteObject(ms, obj);
                array = ms.ToArray();
                result = Encoding.UTF8.GetString(array, 0, array.Length);
            }

            return result;
        }
    }
}
