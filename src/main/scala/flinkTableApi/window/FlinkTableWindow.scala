package flinkTableApi.window

import flinkTableApi.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Over, OverWindowedTable, Session, Slide, Table, Tumble}
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/8/12 14:46:23
 * @Description: FlinkTableWindow
 * @Version 1.0.0
 */
object FlinkTableWindow {
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
    }) // 设置 waterMark
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(2)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })

    // 构建 table
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp.rowtime as 'ts, 'temperature)
    // 行间隔不支持事件时间,只支持处理时间
    //    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id,'ts.proctime, 'temperature)0
    tableEnv.createTemporaryView("sensorTable", sensorTable)

    // -- 开窗  Group Window   行间隔不支持事件时间,只支持处理时间
    // 滚动窗口 Tumble
    val resultGroupTableTumble: Table = sensorTable
      .window(Tumble.over(10.seconds).on('ts).as('tw))
      .groupBy('tw, 'id)
      .select('id, 'id.count as 'num, 'tw.start, 'tw.end, 'tw.rowtime)
    // 滑动窗口 Slide
    val resultGroupTableSlide: Table = sensorTable
      .window(Slide.over(10.seconds).every(5.seconds).on('ts).as('tw))
      //      .window(Slide.over(2.rows).every(1.rows).on('ts).as('tw))
      .groupBy('tw, 'id)
      .select('id, 'id.count as 'num)
    // 会话窗口
    val resultGroupTableSession: Table = sensorTable
      .window(Session.withGap(5.seconds).on('ts).as('tw))
      .groupBy('tw, 'id)
      .select('id, 'id.count as 'num, 'tw.start, 'tw.end, 'tw.rowtime)

    // -- Sql中 Group Window 的 使用
    // TUMBLE(time_attr, interval)  HOP(time_attr, interval, interval)  SESSION(time_attr, interval)
    val resultGroupTableSql: Table = tableEnv.sqlQuery(
      """select id,count(id) as num,
        | TUMBLE_START(ts, INTERVAL '10' second) as wStart,
        | TUMBLE_END(ts, INTERVAL '10' second) as WEND,
        | TUMBLE_ROWTIME(ts, INTERVAL '10' second) as RT
        | from sensorTable group by TUMBLE(ts,INTERVAL '10' second),id
        |""".stripMargin)
    resultGroupTableSql.toRetractStream[Row].print()


    // -- 开窗 Over Window
    // 无界数据流
    //    val resultOverTableUnRange: Table = sensorTable.window(Over.partitionBy('id).orderBy('ts).preceding(UNBOUNDED_RANGE).as('tw))
    //    val resultOverTableUnRow: Table = sensorTable.window(Over.partitionBy('id).orderBy('ts).preceding(UNBOUNDED_ROW).as('tw))
    // 有界数据流   当前行到前5秒的数据
    //    val resultOverTableRange: Table = sensorTable.window(Over.partitionBy('id).orderBy('ts).preceding(5.seconds).as('tw))
    val resultOverTableRow: Table = sensorTable.window(Over.partitionBy('id).orderBy('ts).preceding(2.rows).as('tw))
      .select('id, 'id.count.over('tw).as('num))

    // -- Sql中 Over Window 的 使用
    // 时间间隔
    val resultOverTableRange1: Table = tableEnv.sqlQuery(
      """SELECT id,COUNT(id) OVER (
        |  PARTITION BY id
        |  ORDER BY ts
        |  RANGE BETWEEN INTERVAL '5' second PRECEDING AND CURRENT ROW) as num
        |FROM sensorTable
        |""".stripMargin)
    val resultOverTableRange2: Table = tableEnv.sqlQuery(
      """SELECT id,COUNT(id) OVER w AS num
        |FROM sensorTable
        |WINDOW w AS (
        |  PARTITION BY id
        |  ORDER BY ts
        |  RANGE BETWEEN INTERVAL '5' second PRECEDING AND CURRENT ROW)
        |""".stripMargin)

    // 行间隔
    val resultOverTableRow1: Table = tableEnv.sqlQuery(
      """SELECT id,COUNT(id) OVER (
        |  PARTITION BY id
        |  ORDER BY ts
        |  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as num
        |FROM sensorTable
        |""".stripMargin)
    resultOverTableRow1.toRetractStream[Row].print()


    env.execute("FlinkTableWindow")
  }
}
