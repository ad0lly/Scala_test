import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Ad_test {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount") //部署
    val sc = new SparkContext(config)

    val lineRDD: RDD[String] = sc.textFile("in/agent.log")
    val AdChNumSum: RDD[((String, String), Int)] = lineRDD.map(f = line => {
      val lineSp: Array[String] = line.split(" ")
      ((lineSp(1), lineSp(4)), 1)
    })
    val AdSum: RDD[((String, String), Int)] = AdChNumSum.reduceByKey(_ + _)
    val AdGroup: RDD[(String, (String, Int))] = AdSum.map(x => (x._1._1, (x._1._2, x._2)))
    val AdCh: RDD[(String, Iterable[(String, Int)])] = AdGroup.groupByKey()
    val result: RDD[(String, List[(String, Int)])] = AdCh.mapValues {
      x => x.toList.sortBy(_._2).take(3)
    }
    result.sortBy(_._1).collect().foreach(println)
    result.saveAsObjectFile("output")
  }
}
