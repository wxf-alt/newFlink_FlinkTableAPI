package flinkTableApi

import java.text.SimpleDateFormat
import flinkTableApi.bean.SensorReading
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

//    val path: String = ClassLoader.getSystemResource("sensor.txt").getPath
//    val inputStream: DataStream[String] = env.readTextFile(path)

    // 定义流式数据源
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)

    val dataStream: DataStream[SensorReading] = inputStream.map(x => {
      val str: Array[String] = x.split(" ")
      SensorReading(str(0), str(1).toLong, str(2).toDouble)
    })

    // 创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 基于数据流，转换成一张表，然后进行操作
    val table: Table = tableEnv.fromDataStream(dataStream.map(s => {
      val time: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(s.timestamp * 1000)
      MyFlinkTable(s.id, time, s.temperature)
    }))

    // 调用 Table API,得到转换结果
    val resultTable: Table = table
      .select("id,temperature,time")
      .filter("id = 'sensor_1'")

    // 直接写sql 得到转换结构
    val resultTable1: Table = tableEnv.sqlQuery(s"select id,temperature,`time` from $table where id = 'sensor_1' ")

    // 将表转换为数据流 DataStream，打印输出
    val appendStream: DataStream[(String, Double, String)] = resultTable.toAppendStream[(String, Double, String)]
    val appendStream1: DataStream[(String, Double, String)] = resultTable1.toAppendStream[(String, Double, String)]

//    resultTable.printSchema()
    appendStream.print("appendStream")
    appendStream1.print("appendStream1")

    env.execute("FlinkTableAPITest")
  }
}


case class MyFlinkTable(id: String, time: String, temperature: Double)