import org.apache.spark.sql.SparkSession

object WordCount {
  def wordCount(sc : SparkSession): Unit = {
    val l: List[String] = List("Hello","How are you doing","Let us perform word count","As part of the word count program","we will see how many time each word repeat")
    var l_rdd = sc.sparkContext.parallelize(l)
    val l_map = l_rdd.map(ele => ele.split(" "))  //map function returns an RDD of type Array[String]
    l_map.collect.foreach(println)
    val l_flatMap = l_rdd.flatMap(ele => ele.split(" "))  //flatMap function returns an RDD of type String
    l_flatMap.collect.foreach(println)
    val wordCount = l_flatMap.map(word => (word,1)).countByKey.foreach(println)
    wordCount
  }
}