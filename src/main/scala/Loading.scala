import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

/*This Scala Object loads data from MySQL into spark Dataframes through JDBC connectivity
* JDBC connection dependency is added in build.sbt
* The tables are present in my localhost MySQL DB. They are loaded from MySQL Workbench by using the Import Wizard
* More details in my GitHub: https://github.com/kamireddig/GetDailyRevenue/blob/master/mysql_retail_db_queries
*/
object Loading {
  def departments(sc: SparkSession): DataFrame = {
    var departmentsDF = sc.read.format("jdbc").
      option("url", "jdbc:mysql://localhost:3306/retail_db").
      option("dbtable", "departments").
      option("user", "root").
      option("password", "Summerof69!").
      load()
    departmentsDF
  }
  def customers(sc: SparkSession): DataFrame = {
    var customersDF = sc.read.format("jdbc").
      option("url", "jdbc:mysql://localhost:3306/retail_db").
      option("dbtable", "customers").
      option("user", "root").
      option("password", "Summerof69!").
      load()
    customersDF
  }
  def order(sc: SparkSession) : DataFrame = {
    var orderDF = sc.read.format("jdbc").
      option("url", "jdbc:mysql://localhost:3306/retail_db").
      option("dbtable", "orders").
      option("user", "root").
      option("password", "Summerof69!").
//      load().write.parquet("/data/out")   //Can be written in a specific file format. Parquet file format in this case.
      load()  //We are loading it in default manner
    orderDF
  }
  def categories(sc: SparkSession): DataFrame = {
    var categoriesDF = sc.read.format("jdbc").
      option("url", "jdbc:mysql://localhost:3306/retail_db").
      option("dbtable", "categories").
      option("user", "root").
      option("password", "Summerof69!").
      load()
    categoriesDF
  }
  def products(sc: SparkSession): DataFrame = {
    var productsDF = sc.read.format("jdbc").
      option("url", "jdbc:mysql://localhost:3306/retail_db").
      option("dbtable", "orderItems").
      option("user", "root").
      option("password", "Summerof69!").
      load()
    productsDF
  }
  def orderItems(sc: SparkSession): DataFrame = {
      var orderItemsDF = sc.read.format("jdbc").
        option("url", "jdbc:mysql://localhost:3306/retail_db").
        option("dbtable", "orderItems").
        option("user", "root").
        option("password", "Summerof69!").
        load()
    orderItemsDF
  }
}