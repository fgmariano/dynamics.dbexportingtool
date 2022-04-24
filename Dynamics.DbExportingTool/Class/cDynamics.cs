using Microsoft.Pfe.Xrm;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;
using System.Collections.Generic;
using System.Linq;
using static Dynamics.DbExportingTool.Class.cEnum;

namespace Dynamics.DbExportingTool.Class
{
    public class cDynamics
    {
        private OrganizationServiceManager manager;
        private CrmServiceClient service;

        public cDynamics(bool parallelConn = false)
        {
            service = new CrmServiceClient(cConfig.dynamicsConnection);
            if (parallelConn)
                manager = new OrganizationServiceManager(service);
        }

        public Dictionary<string, object> GetAttributes(string entityName, string[] columns)
        {
            Dictionary<string, object> attr = new Dictionary<string, object>();

            RetrieveEntityRequest request = new RetrieveEntityRequest()
            {
                EntityFilters = EntityFilters.Attributes,
                LogicalName = entityName
            };
            EntityMetadata response = 
                ((RetrieveEntityResponse)service.Execute(request)).EntityMetadata;

            foreach (var attribute in response.Attributes)
            {
                if (columns.Length != 0 && !columns.Contains(attribute.LogicalName))
                    continue;

                if (attribute.AttributeType == AttributeTypeCode.Picklist)
                {
                    attr.Add(attribute.LogicalName, GetPicklistValues(entityName, attribute.LogicalName));
                }
                else
                {
                    DataTypes attrType = DataTypes.Unknown;
                    if (attribute.AttributeType == AttributeTypeCode.String)
                        attrType = DataTypes.String;
                    else if (attribute.AttributeType == AttributeTypeCode.Integer)
                        attrType = DataTypes.Integer;
                    else if (attribute.AttributeType == AttributeTypeCode.BigInt)
                        attrType = DataTypes.BigInt;
                    else if (attribute.AttributeType == AttributeTypeCode.Decimal)
                        attrType = DataTypes.Decimal;
                    else if (attribute.AttributeType == AttributeTypeCode.Money)
                        attrType = DataTypes.Decimal;
                    else if (attribute.AttributeType == AttributeTypeCode.DateTime)
                        attrType = DataTypes.DateTime;
                    else if (attribute.AttributeType == AttributeTypeCode.Boolean)
                        attrType = DataTypes.Boolean;
                    else if (attribute.AttributeType == AttributeTypeCode.Double)
                        attrType = DataTypes.Double;
                    else if (attribute.AttributeType == AttributeTypeCode.Lookup)
                        attrType = DataTypes.Guid;

                    attr.Add(attribute.LogicalName, attrType);
                }
            }
            attr.Remove(entityName + "id");


            return attr;
        }

        public List<Dictionary<string, object>> GetRecords(cDatabase cDatabase, string entityName, Dictionary<string, object> dynCols, string[] columns)
        {
            var records = new List<Dictionary<string, object>>();

            var query = new QueryExpression(entityName);
            if (columns.Length == 0)
            {
                query.ColumnSet.AllColumns = true;
            }
            else
            {
                query.ColumnSet.AddColumns(columns);
            }

            if (!string.IsNullOrEmpty(cConfig.ModifiedOnOrAfter))
                query.Criteria.AddCondition("modifiedon", ConditionOperator.OnOrAfter, cConfig.ModifiedOnOrAfter);

            query.PageInfo = new PagingInfo();
            query.PageInfo.PageNumber = 1;

            int index = 0;
            while (true)
            {
                var response = service.RetrieveMultiple(query);

                if (response.Entities != null)
                {
                    foreach (var item in response.Entities)
                    {
                        var dict = new Dictionary<string, object>();
                        foreach (var attr in item.Attributes)
                        {
                            if (attr.Value.GetType() == typeof(EntityReference))
                                dict.Add(attr.Key, ((EntityReference)attr.Value).Id);
                            else if (attr.Value.GetType() == typeof(OptionSetValue))
                                dict.Add(attr.Key, ((OptionSetValue)attr.Value).Value);
                            else if (attr.Value.GetType() == typeof(Money))
                                dict.Add(attr.Key, ((Money)attr.Value).Value);
                            else
                                dict.Add(attr.Key, attr.Value);
                        }
                        records.Add(dict);
                    }
                }

                if (response.MoreRecords)
                {
                    query.PageInfo.PageNumber++;
                    query.PageInfo.PagingCookie = response.PagingCookie;
                }
                else
                {
                    cDatabase.InsertRows(entityName, dynCols, records);
                    records = new List<Dictionary<string, object>>();
                    break;
                }
                index = index + response.Entities.Count;
                System.Diagnostics.Debug.WriteLine(index);

                if (records.Count == 250000)
                {
                    cDatabase.InsertRows(entityName, dynCols, records);
                    records = new List<Dictionary<string, object>>();
                }
            }

            return records;
        }

        public Dictionary<int, string> GetPicklistValues(string entityName, string attributeName)
        {
            Dictionary<int, string> values = new Dictionary<int, string>();

            PicklistAttributeMetadata results = 
                (PicklistAttributeMetadata)service.GetEntityAttributeMetadataForAttribute
                (entityName, attributeName);

            foreach (var item in results.OptionSet.Options)
            {
                values.Add((int)item.Value, item.Label.UserLocalizedLabel.Label);
            }

            return values;
        }
    }
}