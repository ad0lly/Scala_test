//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.util.parsing.json.JSON
////
////object spark_guangboTest {
////  def main(args: Array[String]): Unit = {
////    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount") //部署
////    val sc = new SparkContext(config)
////    val rdd1: RDD[List[(Int, String)]] = sc.makeRDD(List(List((1, "a"), (2, "b"), (3, "c"))))
////    val list = List((1,1),(2,2),(3,3))
////    val broadcast: Broadcast[Nothing] = sc.broadcast(list)
////    val result: RDD[(Int, (String, Any))] = rdd1.map {
////      case (key, valve) => {
////        var v2: Any = null
////        for (t <- broadcast.value) {
////          if (key == t._1) {
////            v2 = t._2
////          }
////        }
////        (key, (result, v2))
////      }
////    }
////
////
////
//  }
//}
