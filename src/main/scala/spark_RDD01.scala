import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_RDD01 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount") //部署
    val sc = new SparkContext(config)

    val localRDD:RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

    val otherRDD = sc.parallelize(Array(21, 34, 54, 65, 76),1)
    otherRDD.saveAsTextFile("output")

    val ca: RDD[String] = sc.textFile("in")
    localRDD.collect().foreach(println)
    otherRDD.foreach(println)
  }
}
