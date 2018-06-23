using System;
using System.Collections.Generic;
using System.Text;

namespace es.dmoreno.utils.dataaccess.db
{
    public class Utils
    {
        static public string buildInString(string[] array)
        {
            string result;

            result = "";

            for (int i = 0; i < array.Length; i++)
            {
                result += array[i];

                if (i < array.Length - 1)
                {
                    result += ", ";
                }
            }

            return result;
        }

        static internal string buildInString(SQLData data, string field)
        {
            List<string> elements;
            string result;

            elements = new List<string>();
            result = "[";

            while (data.next())
            {
                elements.Add(data.getString(field));
            }

            result += buildInString(elements.ToArray());

            result += "]";

            return result;
        }
    }
}
