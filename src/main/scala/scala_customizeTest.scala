import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

object scala_customizeTest {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_sparkSql") //部署

    val spark: SparkSession = SparkSession.builder().config(config).getOrCreate()

    import spark.implicits._

    //创建聚合函数对象
    val udaf = new MyAgeAvFunctioin
    spark.udf.register("avgAge",udaf)
    val frame: DataFrame = spark.read.json("in/scratch.json")
    frame.createOrReplaceTempView("user")
    spark.sql("select avgAge(age) from user").show()
  }
}

class MyAgeAvFunctioin extends UserDefinedAggregateFunction{
  //函数输入的数据结构
  override def inputSchema: StructType = {
    new StructType().add("age",LongType)

  }
//计算时的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum",LongType)
  }
//函数返回的数据类型

  override def dataType: DataType = DoubleType
//函数是否稳定
  override def deterministic: Boolean = true
//计算之前缓冲区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }
//根据查询结果更新缓冲区数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + input.getLong(1)
  }
//将多个节点的缓冲区进行合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1)  =buffer1.getLong(1) + buffer2.getLong(1)
  }
//计算
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
