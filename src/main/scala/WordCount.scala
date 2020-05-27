import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object WordCount {
  def wordCount(sc : SparkSession): Unit = {
    val l: List[String] = List("Hello","How are you doing","Let us perform word count","As part of the word count program","we will see how many time each word repeat")
    val l_rdd = sc.sparkContext.parallelize(l)
    val l_map = l_rdd.map(ele => ele.split(" "))  //map function returns an RDD of type Array[String]
    l_map.collect.foreach(println)
    val l_flatMap = l_rdd.flatMap(ele => ele.split(" "))  //flatMap function returns an RDD of type String
    l_flatMap.collect.foreach(println)
    val wordCount = l_flatMap.map(word => (word,1)).countByKey
    wordCount.foreach(println)
  }
  def filterMe(sc : SparkSession): Unit = {
    val oDF = Loading.order(sc)
    val oRDD = Loading.orderRDD(sc)

    //Dataframe Filter Condition to filter Completed/Closed orders
    val orderFilterCondDF = oDF.filter((col("order_status") === "COMPLETE" || col("order_status") === "CLOSED") && col("order_date").contains("2013-09"))
    //    orderFilterCondDF.show(10)
    val filterCntDF = orderFilterCondDF.count()
    //    print("Count of orders from Dataframe: " + filterCntDF + "\n\n")

    //RDD Filter Condition to filter Completed/Closed orders
    val completeOrders = oRDD.filter(order => order(3) == "COMPLETE")
    //    println("Completed Orders Count: " + completeOrders.count() + "\n")
    val orderFilterCondRDD = oRDD.filter(order => {
      (order(3) == "COMPLETE" || order(3) == "CLOSED") && order(1).toString.contains("2013-09")
    })
    //    orderFilterCondRDD.take(10).foreach(println)
    val filterCntRDD = orderFilterCondRDD.count
    //    println("\nCount of orders from RDD: " + filterCntRDD)
  }
  def joinMe(sc : SparkSession): Unit = {
    val oRDD = Loading.orderRDD(sc)
    val oIRDD = Loading.orderItemsRDD(sc)

    //Inner Join to join orders and orderItems tables data
    val ordersMap = oRDD.map(o => (o(0),o(1).toString.substring(0,10)))
//    ordersMap.take(5).foreach(println)
//    println("Orders Map Count: " + ordersMap.count() + "\n")
    val orderItemsMap = oIRDD.map(oi => (oi(0), oi(4)))
//    orderItemsMap.take(5).foreach(println)
//    println("Order Items Map Count: " + orderItemsMap.count() + "\n")
    val ordersJoin = ordersMap.join(orderItemsMap)
//    ordersJoin.take(5).foreach(println)
//    println("Orders Inner Joined on OrderItems: " + ordersJoin.count() + "\n")

    //Left Outer Join to join orders and orderItems table data in a left outer join format
    val ordersMap1 = oRDD.map(o => (o(0), o))
    val orderItemsMap1 = oIRDD.map(oi => (oi(0), oi))
    val ordersLeftOuterJoin = ordersMap1.leftOuterJoin(orderItemsMap1)
    ordersLeftOuterJoin.take(5).foreach(println)
    //Filters to only those which has no values in the orderItems table
    val ordersLeftOuterJoinFilter = ordersLeftOuterJoin.filter(order => order._2._2 == None)
    println("Orders Left Outer Joined on OrderItems: " + ordersLeftOuterJoinFilter.count() + "\n")
  }
}
