using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JsonToSQL
{
    public static class Extensions
    {
        public static string ToValidTableName(this string tableName)
        {
            return tableName;
        }
    }
    public class Relations
    {
        public string Source { get; set; }
        public string Target { get; set; }
        public int Order { get; set; }
    }

    public class SqlColumn
    {
        public string Name { get; set; }
        public string Value { get; set; }
        public string Type { get; set; }
    }

    public class JsonToSQL
    {
        public bool ConvertJsonToSQL()
        {
            string json = File.ReadAllText("E:\\json1.txt");

            var objs = JToken.Parse(json);

            if (objs.Type == JTokenType.Object)
            {
                ParseJSON(objs.ToObject<JObject>(), "first", string.Empty, 1, 1);
            }
            else
            {
                var idx = 1;
                
                foreach (var obj in objs.Children<JObject>())
                {                                 
                    ParseJSON(obj, "JsonToSQL", string.Empty, idx, idx);
                    idx++;
                }
            }

            

            foreach (Relations rel in relations.OrderByDescending(i => i.Order))
            {
                if (string.IsNullOrEmpty(rel.Source)) continue;

                DataTable source = ds.Tables[rel.Source];
                DataTable target = ds.Tables[rel.Target];

                if (source == null)
                {
                    target.Columns.Remove(rel.Source + "ID");
                    continue;
                }
               
                source.PrimaryKey = new DataColumn[] { source.Columns[0] };

                ForeignKeyConstraint fk = new ForeignKeyConstraint("ForeignKey", source.Columns[0], target.Columns[1]);
                target.Constraints.Add(fk);
            }

            string schema = CreateScript();

            string data = schema + GetData();

            return true;
        }



        static DataSet ds = new DataSet();
        static List<Relations> relations = new List<Relations>();

        static int index = 0;
        private static void ParseJSON(JObject obj, string tableName, string parentTableName, int pkValue, int fkValue)
        {

            if (obj.Count > 0)
            {
                DataTable dt = new DataTable(tableName);

                Dictionary<string, string> dic = new Dictionary<string, string>();
                List<SqlColumn> listColumns = new List<SqlColumn>();
                //SqlColumn sqlColumn = new SqlColumn();
                //listColumns.Add(sqlColumn);
                //sqlColumn
                listColumns.Add(new SqlColumn { Name = tableName + "ID", Value = pkValue.ToString(), Type = "System.Int32" });
                dic[tableName + "ID"] = pkValue.ToString(); //primary key

                if (!string.IsNullOrEmpty(parentTableName))
                {
                    listColumns.Add(new SqlColumn { Name = parentTableName + "ID", Value = fkValue.ToString(), Type = "System.Int32" });
                    dic[parentTableName + "ID"] = fkValue.ToString(); //foreign key
                }
                
                foreach (JProperty property in obj.Properties())
                {
                    string key = property.Name;
                    JToken jToken = property.Value;
                    
                    if (jToken.Type == JTokenType.Object)
                    {                        
                        var jO = jToken.ToObject<JObject>();
                        ParseJSON(jO, tableName + "_" + key, tableName, pkValue, pkValue);
                        //pkValue = pkValue + 1;
                    }
                    else if (jToken.Type == JTokenType.Array)
                    {
                        var arrs = jToken.ToObject<JArray>();
                        var objects = arrs.Children<JObject>();
                        if (objects.Count() > 0)
                        {
                            //index = 0;
                            foreach (var arr in objects)
                            {
                                index = index + 1;
                                var jo = arr.ToObject<JObject>();
                                ParseJSON(jo, tableName + "_" + key, tableName, index, pkValue);
                            }
                        }
                        else
                        {
                            listColumns.Add(new SqlColumn { Name = key, Value = string.Join(",", arrs.ToObject<string[]>()), Type = "System." + jToken.Type.ToString() });
                            dic[key] = string.Join(",", arrs.ToObject<string[]>());
                        }
                    }
                    else
                    {
                        listColumns.Add(new SqlColumn { Name = key, Value = jToken.ToString(), Type = "System." + jToken.Type.ToString() });
                        dic[key] = jToken.ToString(); 
                    }
                }

                pkValue = pkValue + 1;

                if (ds.Tables.Contains(dt.TableName)) //for array items
                {
                    //ds.Tables[dt.TableName].Rows.Add(dic.Values.ToArray());
                    foreach (string key in dic.Keys)
                    {
                        if (!ds.Tables[dt.TableName].Columns.Contains(key))
                        {
                            ds.Tables[dt.TableName].Columns.Add(AddColumn(key, "System.String", false));
                        }                        
                    }

                    DataRow dr = ds.Tables[dt.TableName].NewRow();
                    foreach (string key in dic.Keys)
                    {
                        dr[key] = dic[key];
                    }

                    ds.Tables[dt.TableName].Rows.Add(dr);
                }
                else if(dic.Keys.Count > 1)
                {
                    for (int i = 0; i < dic.Keys.Count; i++)
                    {
                        string type = i == 0 ? "System.Int32" : "System.String";

                        if (!string.IsNullOrEmpty(parentTableName) && i == 1)
                        {
                            type = "System.Int32"; //foreign key
                        }

                        dt.Columns.Add(AddColumn(dic.Keys.ToArray()[i], type, i == 0 ? true : false));
                    }

                    dt.Rows.Add(dic.Values.ToArray());
                    ds.Tables.Add(dt);

                    relations.Add(new Relations()
                    {
                        Source = parentTableName,
                        Target = tableName,
                        Order = ds.Tables.Count
                    });
                }

            }
        }

        static DataColumn AddColumn(string name, string type, bool isPrimaryKey)
        {
            return new DataColumn()
            {
                ColumnName = name,
                DataType = System.Type.GetType(type),
                AutoIncrement = isPrimaryKey ? true : false,
                AutoIncrementSeed = 1,
                AutoIncrementStep = 1,
                AllowDBNull = true
            };
        }

        static string CreateScript()
        {

            string dbName = "JsonToSQL";
            StringBuilder sb = new StringBuilder();

            sb.AppendFormat("CREATE DATABASE {0}", dbName);
            sb.AppendLine(string.Empty);
            sb.AppendLine("GO" + Environment.NewLine);
            sb.AppendFormat("USE {0}", dbName);
            sb.AppendLine(string.Empty);
            sb.AppendLine("GO" + Environment.NewLine);

            foreach (Relations rel in relations.OrderByDescending(i => i.Order))
            {
                DataTable table = ds.Tables[rel.Target];
                sb.AppendLine(CreateTABLE(table));
                sb.AppendLine("GO");
                sb.AppendLine(string.Empty);
            }

            return sb.ToString();
        }

        private static string GetData()
        {
            StringBuilder sb = new StringBuilder();

            foreach (Relations rel in relations.OrderByDescending(i => i.Order))
            {
                DataTable table = ds.Tables[rel.Target];
                sb.AppendLine($"SET IDENTITY_INSERT [{table.TableName}] ON");

                foreach (DataRow dr in table.Rows)
                {
                    var names = new List<string>();
                    var values = new List<string>();                    

                    foreach (DataColumn column in dr.Table.Columns)
                    {                        
                        names.Add(column.ColumnName);
                        values.Add(GetInsertColumnValue(dr, column));
                    }

                    sb.AppendLine($"INSERT INTO [{table.TableName}] ([{string.Join("], [", names.ToArray())}]) VALUES ({string.Join(", ", values.ToArray())})");
                }

                sb.AppendLine($"SET IDENTITY_INSERT [{table.TableName}] OFF");
                sb.AppendLine("GO");
            }

            return sb.ToString();
        }

        public static string GetInsertColumnValue(DataRow row, DataColumn column)
        {
            string output = "";

            if (row[column.ColumnName] == DBNull.Value)
            {
                output = "NULL";
            }
            else
            {
                if (column.DataType == typeof(bool))
                {
                    output = (bool)row[column.ColumnName] ? "1" : "0";
                }
                else
                {
                    bool addQuotes = false;
                    addQuotes = addQuotes || (column.DataType == typeof(string));
                    addQuotes = addQuotes || (column.DataType == typeof(DateTime));
                    output = row[column.ColumnName].ToString().Replace("'", "''");

                    if (addQuotes)
                    {
                        output = "'" + output + "'";
                    }
                    
                }
            }

            return output;
        }


        public static string CreateTABLE(DataTable table)
        {
            string sqlsc;
            sqlsc = "CREATE TABLE [" + table.TableName + "](";
            for (int i = 0; i < table.Columns.Count; i++)
            {
                sqlsc += "\n [" + table.Columns[i].ColumnName + "]";
                string columnType = table.Columns[i].DataType.ToString();
                switch (columnType)
                {
                    case "System.Int32":
                        sqlsc += " int";
                        break;
                    case "System.Int64":
                        sqlsc += " bigint";
                        break;
                    case "System.Int16":
                        sqlsc += " smallint";
                        break;
                    case "System.Byte":
                        sqlsc += " tinyint";
                        break;
                    case "System.Decimal":
                        sqlsc += " decimal";
                        break;
                    case "System.DateTime":
                        sqlsc += " datetime";
                        break;
                    case "System.Boolean":
                        sqlsc += " bit";
                        break;
                    case "System.String":
                    default:
                        sqlsc += string.Format(" nvarchar({0})", table.Columns[i].MaxLength == -1 ? "max" : table.Columns[i].MaxLength.ToString());
                        break;
                }
                if (table.Columns[i].AutoIncrement)
                    sqlsc += " IDENTITY(" + table.Columns[i].AutoIncrementSeed.ToString() + "," + table.Columns[i].AutoIncrementStep.ToString() + ")";
                if (!table.Columns[i].AllowDBNull)
                    sqlsc += " NOT NULL";
                sqlsc += ",";
            }

            if (table.PrimaryKey.Length > 0)
            {
                StringBuilder primaryKeySql = new StringBuilder();

                primaryKeySql.AppendFormat("\n\tCONSTRAINT [PK_{0}] PRIMARY KEY (", table.TableName);

                for (int j = 0; j < table.PrimaryKey.Length; j++)
                {
                    primaryKeySql.AppendFormat("[{0}],", table.PrimaryKey[j].ColumnName);
                }

                primaryKeySql.Remove(primaryKeySql.Length - 1, 1);
                primaryKeySql.Append(") ");

                //sql.Append(primaryKeySql);
                sqlsc += " " + primaryKeySql;
            }

            sqlsc = sqlsc.Substring(0, sqlsc.Length - 1) + "\n)";

            if (table.Constraints.Count > 0)
            {
                string query = string.Empty;
                foreach (Constraint vv in table.Constraints)
                {
                    if (vv.ConstraintName == "ForeignKey")
                    {
                        ForeignKeyConstraint constraint = vv as ForeignKeyConstraint;
                        query = string.Format("\nALTER TABLE [{0}] WITH CHECK ADD CONSTRAINT [FK_{0}_{1}] FOREIGN KEY([{2}]) REFERENCES [{1}] ([{3}])", constraint.Table.TableName, constraint.RelatedTable.TableName, constraint.Columns[0].ColumnName, constraint.RelatedColumns[0].ColumnName);

                        query += string.Format("\nALTER TABLE [{0}] CHECK CONSTRAINT [FK_{0}_{1}]", constraint.Table.TableName, constraint.RelatedTable.TableName);
                    }

                }

                sqlsc += query;
            }

            return sqlsc;
        }
    }
}
