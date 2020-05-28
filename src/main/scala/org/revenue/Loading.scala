package org.revenue

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/*This Scala Object loads data from MySQL into spark Dataframes through JDBC connectivity
* JDBC connection dependency is added in build.sbt
* The tables are present in my localhost MySQL DB. They are loaded from MySQL Workbench by using the Import Wizard
* More details in my GitHub: https://github.com/kamireddig/GetDailyRevenue/blob/master/mysql_retail_db_queries
*/
object Loading {
  def departments(sc: SparkSession): DataFrame = {
    val departmentsDF = sc.read.format("jdbc").
      option("url", "jdbc:mysql://localhost:3306/retail_db?autoReconnect=true&useSSL=false").
      option("dbtable", "departments").
      option("user", "root").
      option("password", "Summerof69!").
      load()

    departmentsDF     //Return keyword not mandatory in Scala as Scala is a Functional Programming language
  }
  //Creating RDD from above Dataframe using .rdd method
  def departmentRDD(sc : SparkSession): RDD[Row] = {
    val deptRDD = departments(sc).rdd
    deptRDD
  }
  def customers(sc: SparkSession): DataFrame = {
    val customersDF = sc.read.format("jdbc").
      option("url", "jdbc:mysql://localhost:3306/retail_db?autoReconnect=true&useSSL=false").
      option("dbtable", "customers").
      option("user", "root").
      option("password", "Summerof69!").
      load()

    customersDF
  }
  def customersRDD(sc : SparkSession): RDD[Row] = {
    val custRDD = customers(sc).rdd
    custRDD
  }
  def order(sc: SparkSession) : DataFrame = {
    val orderDF = sc.read.format("jdbc").
      option("url", "jdbc:mysql://localhost:3306/retail_db?autoReconnect=true&useSSL=false").
      option("dbtable", "orders").
      option("user", "root").
      option("password", "Summerof69!").
//      load().write.parquet("/data/out")   //Can be written in a specific file format. Parquet file format in this case.
      load()  //We are loading it in default manner

    orderDF
  }
  def orderRDD(sc : SparkSession): RDD[Row] = {
    val ordRDD = order(sc).rdd
    ordRDD
  }
  def categories(sc: SparkSession): DataFrame = {
    val categoriesDF = sc.read.format("jdbc").
      option("url", "jdbc:mysql://localhost:3306/retail_db?autoReconnect=true&useSSL=false").
      option("dbtable", "categories").
      option("user", "root").
      option("password", "Summerof69!").
      load()

    categoriesDF
  }
  def categoriesRDD(sc : SparkSession): RDD[Row] = {
    val categRDD = order(sc).rdd
    categRDD
  }
  def products(sc: SparkSession): DataFrame = {
    val productsDF = sc.read.format("jdbc").
      option("url", "jdbc:mysql://localhost:3306/retail_db?autoReconnect=true&useSSL=false").
      option("dbtable", "orderItems").
      option("user", "root").
      option("password", "Summerof69!").
      load()

    productsDF
  }
  def productsRDD(sc : SparkSession): RDD[Row] = {
    val prdRDD = products(sc).rdd
    prdRDD
  }
  def orderItems(sc: SparkSession): DataFrame = {
    val orderItemsDF = sc.read.format("jdbc").
      option("url", "jdbc:mysql://localhost:3306/retail_db?autoReconnect=true&useSSL=false").
      option("dbtable", "orderItems").
      option("user", "root").
      option("password", "Summerof69!").
      load()

    orderItemsDF
  }
  def orderItemsRDD(sc : SparkSession): RDD[Row] = {
    val ordItRDD = orderItems(sc).rdd
    ordItRDD
  }
}
