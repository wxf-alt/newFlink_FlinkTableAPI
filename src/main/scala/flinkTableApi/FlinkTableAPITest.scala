package flinkTableApi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.Table

/**
 * @Auther: wxf
 * @Date: 2022/7/25 19:53:48
 * @Description: FlinkTableAPITest
 * @Version 1.0.0
 */
object FlinkTableAPITest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val path: String = ClassLoader.getSystemResource("sensor.txt").getPath
    val inputStream: DataStream[String] = env.readTextFile(path)
    val dataStream: DataStream[SensorReading] = inputStream.map(x => {
      val str: Array[String] = x.split(" ")
      SensorReading(str(0), str(1).toLong, str(2).toDouble)
    })

    // 创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 基于数据流，转换成一张表，然后进行操作
    val table: Table = tableEnv.fromDataStream(dataStream)

    // 调用 Table API,得到转换结果
    val resultTable: Table = table
      .addColumns("timestamp as time")
      .select("id,temperature,time")
      .filter("id = 'sensor_1'")

    // 将表转换为数据流 DataStream，打印输出
    val appendStream: DataStream[(String, Double, Long)] = resultTable.toAppendStream[(String, Double, Long)]

    resultTable.printSchema()
    appendStream.print("appendStream")

    env.execute("FlinkTableAPITest")
  }
}
