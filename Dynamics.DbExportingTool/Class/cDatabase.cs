using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using static Dynamics.DbExportingTool.Class.cEnum;

namespace Dynamics.DbExportingTool.Class
{
    public class cDatabase
    {
        private string CreatePicklistTable(string tableName, Dictionary<int, string> items)
        {
            string sql = $"CREATE TABLE {tableName} (id INT, name VARCHAR(50)) " +
                $"INSERT INTO {tableName} VALUES ";
            foreach (var item in items)
            {
                sql += $"({item.Key}, '{item.Value}'),";
            }
            sql = sql.TrimEnd(new char[] { ',' }) + " ";

            return sql;
        }

        public bool CreateTableIfNotExists(string entityName, Dictionary<string, object> columns)
        {
            string sql = $"IF NOT EXISTS (SELECT * FROM sysobjects WHERE " +
                $"name='{entityName}' and xtype='U') BEGIN CREATE TABLE {entityName} (" +
                $"{entityName}id varchar(36) PRIMARY KEY, ";
            string additionalTables = "";

            foreach (var column in columns.Keys)
            {
                string datatype = "varchar(max)";
                if (columns[column].GetType() == typeof(DataTypes))
                {
                    if ((DataTypes)columns[column] == DataTypes.String)
                        datatype = "VARCHAR(MAX)";
                    else if ((DataTypes)columns[column] == DataTypes.Integer)
                        datatype = "INT";
                    else if ((DataTypes)columns[column] == DataTypes.BigInt)
                        datatype = "BIGINT";
                    else if ((DataTypes)columns[column] == DataTypes.DateTime)
                        datatype = "DATETIME";
                    else if ((DataTypes)columns[column] == DataTypes.Boolean)
                        datatype = "BIT";
                    else if ((DataTypes)columns[column] == DataTypes.Decimal)
                        datatype = "DECIMAL";
                    else if ((DataTypes)columns[column] == DataTypes.Double)
                        datatype = "FLOAT";
                    else if ((DataTypes)columns[column] == DataTypes.Guid)
                        datatype = "VARCHAR(36)";
                }
                else
                {
                    datatype = "INT";
                    additionalTables += CreatePicklistTable($"{entityName}_{column}", 
                        (Dictionary<int, string>)columns[column]);
                }
                sql += $"{column} {datatype},";
            }
            sql = sql.TrimEnd(new char[] { ',' }) + ") " + additionalTables;
            sql = sql.TrimEnd() + " END";

            SqlConnection conn = new SqlConnection(cConfig.dbConnection);
            conn.Open();

            SqlCommand command = conn.CreateCommand();
            command.CommandText = sql;
            command.CommandType = CommandType.Text;

            int result = command.ExecuteNonQuery();
            conn.Close();

            if (result > 0)
                return true;
            else
                return false;
        }

        public void InsertRows(string tableName, Dictionary<string, object> columnList, 
            List<Dictionary<string, object>> records)
        {
            DataTable dt = new DataTable();
            dt.Columns.Add(tableName + "id", typeof(string));
            foreach (var item in columnList)
            {
                if (item.Value.GetType() == typeof(DataTypes))
                {
                    if ((DataTypes)item.Value == DataTypes.BigInt)
                        dt.Columns.Add(item.Key, typeof(long));
                    else if ((DataTypes)item.Value == DataTypes.String)
                        dt.Columns.Add(item.Key, typeof(string));
                    else if ((DataTypes)item.Value == DataTypes.Boolean)
                        dt.Columns.Add(item.Key, typeof(bool));
                    else if ((DataTypes)item.Value == DataTypes.DateTime)
                        dt.Columns.Add(item.Key, typeof(DateTime));
                    else if ((DataTypes)item.Value == DataTypes.Decimal)
                        dt.Columns.Add(item.Key, typeof(decimal));
                    else if ((DataTypes)item.Value == DataTypes.Double)
                        dt.Columns.Add(item.Key, typeof(double));
                    else if ((DataTypes)item.Value == DataTypes.Guid)
                        dt.Columns.Add(item.Key, typeof(string));
                    else if ((DataTypes)item.Value == DataTypes.Integer)
                        dt.Columns.Add(item.Key, typeof(int));
                    else if ((DataTypes)item.Value == DataTypes.Unknown)
                        dt.Columns.Add(item.Key, typeof(string));
                }
                else
                {
                    dt.Columns.Add(item.Key, typeof(string));
                }
            }

            DataRow row;
            foreach (var item in records)
            {
                row = dt.NewRow();
                foreach (var attribute in item.Keys)
                {
                    row[attribute] = item[attribute];
                }
                dt.Rows.Add(row);
            }

            SqlConnection conn = new SqlConnection(cConfig.dbConnection);
            conn.Open();

            int tries = 1;
            Exception error = null;
            while (true)
            {
                conn = new SqlConnection(cConfig.dbConnection);
                conn.Open();

                if (tries == 6)
                {
                    cLogging.Log("BulkInsert", error.StackTrace);
                    throw error;
                }

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn))
                {
                    bulkCopy.BatchSize = 50000;
                    bulkCopy.BulkCopyTimeout = 0;
                    bulkCopy.DestinationTableName = tableName;

                    try
                    {
                        bulkCopy.WriteToServer(dt);
                        break;
                    }
                    catch (Exception ex)
                    {
                        tries++;
                        error = ex;
                        conn.Dispose();
                        conn.Close();
                    }
                }
            }

            conn.Close();
        }
    }
}