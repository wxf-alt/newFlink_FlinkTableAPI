package flinkTableApi.function

import flinkTableApi.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/8/17 19:44:35
 * @Description: TableFunctionTest  表函数
 * @Version 1.0.0
 */
object TableFunctionTest {
  def main(args: Array[String]): Unit = {
    val path: String = ClassLoader.getSystemResource("sensor.txt").getPath

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    val inputStream: DataStream[String] = env.readTextFile(path)
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(" ")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    }) // 设置 waterMark
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(2)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })

    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp.rowtime as 'ts, 'temperature)

    val split: Split = new Split("_")
    // 调用 Table API 中 TableFunction 使用的时候,需要调用 joinLateral 或者 leftOuterJoinLateral方法
    val resultTable: Table = sensorTable
      .joinLateral(split('id) as ('word, 'length))
      .select('id, 'ts, 'word, 'length)

    // SQL 方式 调用 TableFunction
    tableEnv.registerFunction("split", split)
    tableEnv.createTemporaryView("sensorTable", sensorTable)
    val resultSql: Table = tableEnv.sqlQuery(
      """select id, ts, word, length
        |from sensorTable, lateral table(split(id)) as splited(word,length)
        |""".stripMargin)

    resultSql.toAppendStream[Row].print("result：")
    env.execute("TableFunctionTest")
  }

  // 将传入的数据 用指定分隔符进行分割，并包装成元组输出
  class Split(separator: String) extends TableFunction[(String, Int)] {
    def eval(str: String): Unit = {
      str.split(separator).foreach(
        word => collect(word, word.length)
      )
    }
  }

}