import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.helper.Helper._
import spire.syntax.order

object GetDailyRevenue {
  def main(args: Array[String]): Unit = {
    suppressLogs(List("org", "akka"))     //removes log info in console

    //Creating a SparkSession
    var sc = SparkSession.builder()
      .appName("Get Daily Revenue") //Name of the application. Appears in UI and log data.
      .master("local")       //set master as yarn. yarn will manage the resources. You can also set it as local (Default) when running locally.
      .config("spark.ui.port",12345)       //sets the port to 12345 to check from UI through this port. Default is 4040.
      .config("spark.executor.memory","512mb") //512MB per executor process. Default is 1GB.
      .config("spark.logConf",value = true)
      .config("spark.eventLog.enabled",value = true)
      .enableHiveSupport()
      .getOrCreate()

    //Creating instances of the tables: Dataframes created in the class Loading
    val departmentsDF = Loading.departments(sc)
    val customersDF = Loading.customers(sc)
    val orderDF = Loading.order(sc)
    val categoriesDF = Loading.categories(sc)
    val productsDF = Loading.products(sc)
    val orderItemsDF = Loading.orderItems(sc)
//    departmentsDF.show()

    /* Cannot create RDD from external Databases. Can only create from external datasets like text files, parquet files, avro files, etc.,
     * from existing RDDs
     * and by using parallelize method
     */

    //Creating RDD's from the above Dataferames using .rdd method
    val departmentsRDD = departmentsDF.rdd
    val customersRDD = customersDF.rdd
    val orderRDD = orderDF.rdd
    val categoriesRDD = categoriesDF.rdd
    val productsRDD = productsDF.rdd
    val orderItemsRDD = orderItemsDF.rdd

//    val wordCountDF = WordCount.wordCount(sc)

    //Dataframe Filter Condition to filter Completed orders
//    orderDF.filter("order_status == 'CLOSED'").show()
//    orderDF.filter("order_status == 'COMPLETE' or order_status == 'CLOSED' and order_date.toString.contains('2013-07')").show()
//      " && order_date.contains('2013-09')").show()

//    val completeOrders = orderRDD.filter(order => order(3)=="COMPLETE")
    val cond = orderRDD.filter(order => {
      order(3) == "COMPLETE" || order(3) == "CLOSED" && order(1).toString.contains("2013-09")
    })
    cond.take(10).foreach(println)
  }
}