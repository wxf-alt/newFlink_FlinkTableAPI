package flinkTableApi.window

import java.sql.Timestamp

import flinkTableApi.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Over, Table, Tumble}
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/8/5 19:28:50
 * @Description: FlinkTableGroupWindow   开一个滚动窗口，统计10秒内出现的每个sensor的个数
 * @Version 1.0.0
 */
object FlinkTableOverWindow {
  def main(args: Array[String]): Unit = {
    val path: String = ClassLoader.getSystemResource("sensor.txt").getPath

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    val inputStream: DataStream[String] = env.readTextFile(path)
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(" ")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      // 设置 waterMark
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(2)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })

    // 构建 table
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp.rowtime as 'ts, 'temperature)
    tableEnv.createTemporaryView("sensorTable", sensorTable)

    // (1) FlinkTable 方式
    // 无界数据流
    //    val resultOverTable: Table = sensorTable.window(Over.partitionBy('id).orderBy('ts).preceding(UNBOUNDED_RANGE).as('tw))
    //    val resultOverTable: Table = sensorTable.window(Over.partitionBy('id).orderBy('ts).preceding(UNBOUNDED_ROW).as('tw))
    // 有界数据流
    //    val resultOverTable: Table = sensorTable.window(Over.partitionBy('id).orderBy('ts).preceding(5.seconds).as('tw))
    //    val resultOverTable: Table = sensorTable.window(Over.partitionBy('id).orderBy('ts).preceding(2.rows).as('tw))
    //      .select('id, 'id.count.over('tw).as('num))

    //    // (2) 使用 Sql 方式
    //    // 时间间隔
    //    val resultOverTable: Table = tableEnv.sqlQuery(
    //      """SELECT id,COUNT(id) OVER (
    //        |  PARTITION BY id
    //        |  ORDER BY ts
    //        |  ROWS BETWEEN INTERVAL '5' second PRECEDING AND CURRENT ROW) as num
    //        |FROM sensorTable
    //        |""".stripMargin)
    //    val resultOverTable: Table = tableEnv.sqlQuery(
    //      """SELECT id,COUNT(id) OVER w AS num
    //        |FROM sensorTable
    //        |WINDOW w AS (
    //        |  PARTITION BY id
    //        |  ORDER BY ts
    //        |  RANGE BETWEEN INTERVAL '5' second PRECEDING AND CURRENT ROW)
    //        |""".stripMargin)

    //      // 行间隔
    val resultOverTable: Table = tableEnv.sqlQuery(
      """SELECT id,COUNT(id) OVER (
        |  PARTITION BY id
        |  ORDER BY ts
        |  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as num
        |FROM sensorTable
        |""".stripMargin)
    resultOverTable.toRetractStream[Row].print()

    env.execute("FlinkTableGroupWindow")
  }
}