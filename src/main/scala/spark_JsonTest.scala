import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object spark_JsonTest {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount") //部署
    val sc = new SparkContext(config)

    val json: RDD[String] = sc.textFile("in/scratch.json")
    val result: RDD[Option[Any]] = json.map(JSON.parseFull)
    result.foreach(println)

  }
}
