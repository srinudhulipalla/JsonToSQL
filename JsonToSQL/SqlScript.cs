using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Text;

namespace JsonToSQL
{
    internal class SqlScript
    {
        public static string GenerateDbSchema(DataSet ds, List<TableRelation> relations)
        {

            string dbName = "JsonToSQL";
            StringBuilder sb = new StringBuilder();

            sb.AppendFormat("CREATE DATABASE {0}", dbName);
            sb.AppendLine(string.Empty);
            sb.AppendLine("GO" + Environment.NewLine);
            sb.AppendFormat("USE {0}", dbName);
            sb.AppendLine(string.Empty);
            sb.AppendLine("GO" + Environment.NewLine);

            foreach (TableRelation rel in relations.OrderByDescending(i => i.Order))
            {
                DataTable table = ds.Tables[rel.Target];
                sb.AppendLine(CreateTABLE(table));
                sb.AppendLine("GO");
                sb.AppendLine(string.Empty);
            }

            return sb.ToString();
        }

        static string CreateTABLE(DataTable table)
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

        public static string GenerateInsertQueries(DataSet ds, List<TableRelation> relations)
        {
            StringBuilder sb = new StringBuilder();

            foreach (TableRelation rel in relations.OrderByDescending(i => i.Order))
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

        static string GetInsertColumnValue(DataRow row, DataColumn column)
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

    }
}
