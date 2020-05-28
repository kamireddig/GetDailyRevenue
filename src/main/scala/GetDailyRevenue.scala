import org.apache.spark.sql.{Row, SparkSession}
import org.helper.Helper._
import org.apache.spark.sql.functions.{col, udf}

object GetDailyRevenue {
  def main(args: Array[String]): Unit = {
    suppressLogs(List("org", "akka"))     //removes log info in console

    //Creating a SparkSession
    val sc = SparkSession.builder()
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

    /* We Cannot create RDD from external Databases. Can only create from external datasets like text files, parquet files, avro files, etc.,
     * from existing RDDs
     * and by using parallelize method
     */

    //Creating RDD's from the above Dataferames using .rdd method
    val deptRDD = Loading.departmentRDD(sc)
    val custRDD = Loading.customersRDD(sc)
    val ordRDD = Loading.orderRDD(sc)
    val categRDD = Loading.categoriesRDD(sc)
    val prdRDD = Loading.productsRDD(sc)
    val ordItRDD = Loading.orderItemsRDD(sc)

//    val wordCountDF = FunctionMe.wordCount(sc)
    val filterM : Unit = FunctionMe.filterMe(sc)
    val joinMetaData : Unit = FunctionMe.joinMe(sc)
  }
}