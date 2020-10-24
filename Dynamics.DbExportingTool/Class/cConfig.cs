using System.Configuration;

namespace Dynamics.DbExportingTool.Class
{
    public class cConfig
    {
        public static string dbConnection
        {
            get
            {
                if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["dbConnection"]))
                    return ConfigurationManager.AppSettings["dbConnection"];
                return "";
            }
        }

        public static string dynamicsConnection
        {
            get
            {
                return $"AuthType=Office365;Username={dynamicsUser};Password={dynamicsPass};Url={dynamicsHost}";
            }
        }

        public static string dynamicsHost
        {
            get
            {
                if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["dynamicsHost"]))
                    return ConfigurationManager.AppSettings["dynamicsHost"];
                return "";
            }
        }

        public static string dynamicsUser
        {
            get
            {
                if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["dynamicsUser"]))
                    return ConfigurationManager.AppSettings["dynamicsUser"];
                return "";
            }
        }

        public static string dynamicsPass
        {
            get
            {
                if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["dynamicsPass"]))
                    return ConfigurationManager.AppSettings["dynamicsPass"];
                return "";
            }
        }

        public static string[] entities
        {
            get
            {
                if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["entities"]))
                    return ConfigurationManager.AppSettings["entities"].Split(',');
                return new string[0];
            }
        }

        public static string[] columns
        {
            get
            {
                if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["columns"]))
                    return ConfigurationManager.AppSettings["columns"].Split(',');
                return new string[0];
            }
        }

        public static string ModifiedOnOrAfter
        {
            get
            {
                if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["modifiedOnOrAfter"]))
                    return ConfigurationManager.AppSettings["modifiedOnOrAfter"];
                return "";
            }
        }
    }
}