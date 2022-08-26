package flinkTableApi.window

import java.sql.Timestamp

import flinkTableApi.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table, Tumble}
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/8/5 19:28:50
 * @Description: FlinkTableGroupWindow   开一个滚动窗口，统计10秒内出现的每个sensor的个数
 * @Version 1.0.0
 */
object FlinkTableGroupWindow {
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
    val inputTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp.rowtime, 'temperature)

    // (1) FlinkTable 方式
    val resultAPITable: Table = inputTable
      //      .window(Tumble.over(10.seconds).on('timestamp).as('tw))
      .window(Tumble over 10.seconds on 'timestamp as 'tw) // 10 秒钟的滚动窗口 Tumble
      .groupBy('tw, 'id)
      // 获取 窗口的起始时间，窗口的结束时间，窗口的截至eventTime
      .select('id, 'id.count as 'num, 'tw.start)

    resultAPITable.toRetractStream[Row].print("resultAPITable：")

    // (2) 使用 Sql 方式
    val sqlDataTable: Table = inputTable.select('id, 'timestamp as 'ts, 'temperature)
    val resultSQLTable: Table = tableEnv.sqlQuery("select id,count(id) as num,tumble_start(ts,interval '10' second) from " + sqlDataTable + " group by id,tumble(ts,interval '10' second)")

    resultSQLTable.toRetractStream[Row].print("resultSQLDstream：")

    env.execute("FlinkTableGroupWindow")
  }
}