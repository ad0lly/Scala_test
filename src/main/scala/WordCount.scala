import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount") //部署
    val sc = new SparkContext(config)
    println(sc)
    val word:Unit = sc.textFile("file:/C:/Users/Lost_/IdeaProjects/untitled6/in/word.text")
                              .flatMap(_.split(" "))
                              .map((_, 1))
                              .reduceByKey((_ + _))
                              .collect()
                              .foreach(println)
    println(word)

  }
}