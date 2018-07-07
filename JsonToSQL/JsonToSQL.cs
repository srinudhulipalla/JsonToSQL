using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace JsonToSQL
{   
    public class JsonConvert
    {
        public string DatabaseName { get; set; }

        static DataSet ds = new DataSet();
        static List<TableRelation> relations = new List<TableRelation>();

        static int index = 0;

        public string ToSQL(string json)
        {
            if (string.IsNullOrWhiteSpace(this.DatabaseName))
            {
                this.DatabaseName = "JsonToSQL";
            }

            var jToken = JToken.Parse(json);

            if (jToken.Type == JTokenType.Object) //single json object 
            {
                ParseJObject(jToken.ToObject<JObject>(), this.DatabaseName, string.Empty, 1, 1);
            }
            else //multiple json objects in array 
            {
                var counter = 1;
                
                foreach (var jObject in jToken.Children<JObject>())
                {                                 
                    ParseJObject(jObject, this.DatabaseName, string.Empty, counter, counter);
                    counter++;
                }
            }

            foreach (TableRelation rel in relations.OrderByDescending(i => i.Order))
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

            string schema = SqlScript.GenerateDbSchema(ds, relations);

            string data = schema + SqlScript.GenerateInsertQueries(ds, relations);

            return data;
        }


        private void ParseJObject(JObject jObject, string tableName, string parentTableName, int pkValue, int fkValue)
        {
            if (jObject.Count > 0)
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
                
                foreach (JProperty property in jObject.Properties())
                {
                    string key = property.Name;
                    JToken jToken = property.Value;
                    
                    if (jToken.Type == JTokenType.Object)
                    {                        
                        var jO = jToken.ToObject<JObject>();
                        ParseJObject(jO, tableName + "_" + key, tableName, pkValue, pkValue);
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
                                ParseJObject(jo, tableName + "_" + key, tableName, index, pkValue);
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

                    relations.Add(new TableRelation()
                    {
                        Source = parentTableName,
                        Target = tableName,
                        Order = ds.Tables.Count
                    });
                }

            }
        }

        DataColumn AddColumn(string name, string type, bool isPrimaryKey)
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

        

        

       


        
    }
}
