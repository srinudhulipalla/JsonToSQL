using System;
using System.IO;
using JsonToSQL;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace JsonToSQLUnitTest
{
    [TestClass]
    public class JsonToSQLTest
    {
        [TestMethod]
        public void ConvertJsonToSQL()
        {
            JsonConvert obj = new JsonConvert();
            obj.DatabaseName = "JsonToSQL1";

            string json = File.ReadAllText("E:\\json1.txt");

            string result = obj.ToSQL(json);

            

            Assert.AreEqual(null, null);
        }
    }
}
