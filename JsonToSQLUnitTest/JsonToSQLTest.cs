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
        public void ConvertJsonStringToSQL()
        {
            //Json string to SQL script
            JsonConvert converter = new JsonConvert();      
            
            string json = File.ReadAllText("E:\\json1.txt");

            string sqlScript = converter.ToSQL(json);


            Assert.AreEqual(null, null);
        }

        [TestMethod]
        public void ConvertJsonStreamToSQL()
        {
            //Json stream to SQL script
            JsonConvert converter = new JsonConvert();

            string jsonFilePath = "E:\\json1.txt";

            using (FileStream fs = File.OpenRead(jsonFilePath))
            {                
                MemoryStream ms = new MemoryStream();
                ms.SetLength(fs.Length);                
                fs.Read(ms.GetBuffer(), 0, (int)fs.Length);

                string sqlScript = converter.ToSQL(ms);
            }


            Assert.AreEqual(null, null);
        }

    }
}
