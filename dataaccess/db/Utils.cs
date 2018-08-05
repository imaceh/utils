using System;
using System.Collections.Generic;
using System.Reflection;
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

        static public List<PropertyInfo> getPropertyInfos<T>(T reg, bool with_fieldattribute = false) where T : class, new()
        {
            List<PropertyInfo> result;

            var properties = reg.GetType().GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);

            result = new List<PropertyInfo>(properties.Length);

            foreach (var item in properties)
            {
                if (with_fieldattribute)
                {
                    var att = item.GetCustomAttribute<FieldAttribute>();

                    if (att != null)
                    {
                        result.Add(item);
                    }
                }
                else
                {
                    result.Add(item);
                }
            }

            return result;
        }

        static public List<FieldAttribute> getFieldAttributes(List<PropertyInfo> p)
        {
            var result = new List<FieldAttribute>(p.Count);

            foreach (var item in p)
            {
                var att = item.GetCustomAttribute<FieldAttribute>();

                if (att != null)
                {
                    result.Add(att);
                }
            }

            return result;
        }

        static public List<ConstraintAttribute> getFieldConstraints(List<PropertyInfo> p)
        {
            var result = new List<ConstraintAttribute>();

            foreach (var item in p)
            {
                var consts = item.GetCustomAttributes<ConstraintAttribute>();
                var field_att = item.GetCustomAttribute<FieldAttribute>();

                foreach (var cons in consts)
                {
                    if (field_att != null)
                    {
                        cons.FieldName = field_att.FieldName;
                    }
                    result.Add(cons);
                }
            }

            return result;
        }
    }
}
