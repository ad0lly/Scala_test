import java.sql.DriverManager

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object spark_JdbcTest {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount") //部署
    val sc = new SparkContext(config)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/sys"
    val userName = "root"
    val passWd = "19980418xyz"
    val sql = "select id,name from user"
    new JdbcRDD(
      sc,
      () =>{
        Class.forName(driver)
        DriverManager.getConnection(url,userName,passWd)
      },
      sql,
      1,
      3,
      3,
      (rs)=>{
        println(rs.getInt(1) + "," + rs.getString(2))
      }
    ).collect()
      sc.stop()
  }
}
