import java.sql.DriverManager

import org.apache.spark
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object spark_sparkSql {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_sparkSql") //部署

    val Session: SparkSession = SparkSession.builder().config(config).getOrCreate()

    val frame: DataFrame = Session.read.json("in/scratch.json")
    frame.createGlobalTempView("user")
   //spark.sql("select * from user").show
    //frame.show()

    Session.stop()

  }
}
