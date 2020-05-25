import org.apache.spark.sql.SparkSession
import org.helper.Helper._

object GetDailyRevenue {
  def main(args: Array[String]): Unit = {
    suppressLogs(List("org", "akka"))     //removes log info in console

    var sc = SparkSession.builder()
      .appName("Get Daily Revenue") //Name of the application. Appears in UI and log data.
      .master("local")       //set master as yarn. yarn will manage the resources. You can also set it as local (Default) when running locally.
      .config("spark.ui.port",12345)       //sets the port to 12345 to check from UI through this port. Default is 4040.
      .config("spark.executor.memory","512mb") //512MB per executor process. Default is 1GB.
      .config("spark.logConf",value = true)
      .config("spark.eventLog.enabled",value = true)
      .enableHiveSupport()
      .getOrCreate()

    var departmentsDF = Loading.departments(sc)
    var customersDF = Loading.customers(sc)
    var orderDF = Loading.order(sc)
    var categoriesDF = Loading.categories(sc)
    var productsDF = Loading.products(sc)
    var orderItemsDF = Loading.orderItems(sc)

    departmentsDF.show()
  }
}