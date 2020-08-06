import java.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_RDD02 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount") //部署
    val sc = new SparkContext(config)

    val parallelizeRDD: RDD[(String,Int)] = sc.parallelize(List(("a", 5), ("a",9), ("b", 3), ("d", 2), ("d", 9)), 2)
     // .aggregateByKey(0)(math.max(_,_),_+_)
    //  .combineByKey((_,1),a)

    //    val pRdd: Array[Array[(String, Int)]] = parallelizeRDD.glom().collect()
//    pRdd.foreach(println)
//    val listRdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
//
//      val partRDD: RDD[(String, Int)] = listRdd.partitionBy(new Mypartition(3))
//    partRDD.saveAsTextFile("output")
//
//      class Mypartition(Partitions:Int) extends org.apache.spark.Partitioner{
//        override def numPartitions: Int = {
//          Partitions
//        }
//
//        override def getPartition(key: Any): Int = {
//          1
//        }
//      }
//
//          val secRDD: Array[(Int, Int)] = sc.makeRDD(11 to 20).map(x => (x,1))
//            .collect().

//    val triRDD: RDD[Int] = listRDD.union(secRDD)


//    println(listRDD.partitions.size)
//
//    val coalesceRDD: RDD[Int] = listRDD.coalesce(3)
//
//    println(coalesceRDD.partitions.size)
//    coalesceRDD
//      .sample(true,0.4,System.currentTimeMillis())
//      .mapPartitionsWithIndex{
//        case(num,dates) => {
//          dates.map((_,"分区号:" + num))
//        }
//      }
//      .mapPartitions(dates=> {
//        dates.map(_*2)
//      })
      //.collect().foreach(println)
//         .map(x => x * 2)
//         .collect().foreach(println)

  }
}
