import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object scala_Transform {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_sparkSql") //部署

    val spark: SparkSession = SparkSession.builder().config(config).getOrCreate()

    import spark.implicits._

    val listRDD: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "ansuyu", 22), (2, "wangbin", 20), (3, "eason", 30)))
//
//    val df = listRDD.toDF("id","name","age")//定义结构
//
//    val ds: Dataset[User] = df.as[User]
//
//    val df1: DataFrame = ds.toDF()
//
//    val rdd1: RDD[Row] = df1.rdd
//
//      rdd1.foreach(row => {
//        println(row.getString(1))
//      })
val userRDD: RDD[User] = listRDD.map {
  case (id, name, age) => {
    User(id, name, age)
  }
}
    val userDs: Dataset[User] = userRDD.toDS()
    userDs.show()
    val newRDD: RDD[User] = userDs.rdd
    newRDD.foreach(println)
  }
}
case class User(id:Int,name:String,age:Int)