using System;

namespace es.dmoreno.utils.path
{
    public class PathHelper
    {
        static public string getAppDataFolder()
        {
            var path = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
            return path;
        }
    }
}
