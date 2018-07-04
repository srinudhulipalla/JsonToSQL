using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace JsonToSQLUnitTest
{
    [TestClass]
    public class JsonToSQLTest
    {
        [TestMethod]
        public void ConvertJsonToSQL()
        {
            JsonToSQL.JsonToSQL obj = new JsonToSQL.JsonToSQL();

            bool result = obj.ConvertJsonToSQL();

            Assert.AreEqual(true, result);
        }
    }
}
