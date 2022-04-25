using Dynamics.DbExportingTool.Class;
using System.Web.Http;

namespace Dynamics.DbExportingTool.Controllers
{
    public class MainController : ApiController
    {
        public string Get()
        {
            cDatabase db = new cDatabase();
            cDynamics dyn = new cDynamics();

            foreach (var item in cConfig.entities)
            {
                System.Diagnostics.Debug.WriteLine($"processing {item}");
                try
                {
                    var attr = dyn.GetAttributes(item, cConfig.Columns(item));
                    var response = db.CreateTableIfNotExists(item, attr);
                    var arr = dyn.GetRecords(db, item, attr, cConfig.Columns(item));
                }
                catch (System.Exception e)
                {
                    return e.Message + "::" + e.StackTrace;
                }
            }

            return "ok";
        }
    }
}
