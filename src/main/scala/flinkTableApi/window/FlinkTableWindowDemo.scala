package flinkTableApi.window

import flinkTableApi.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row


/**
 * @Auther: wxf
 * @Date: 2022/8/12 14:46:23
 * @Description: FlinkTableWindow  统计10秒内出现的每个sensor的个数。
 * @Version 1.0.0
 */
object FlinkTableWindowDemo {
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

    // 构建 table     Event Time
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp.rowtime as 'ts, 'temperature)
    // process Time
    //    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'ts.proctime)
    sensorTable.toAppendStream[Row].print("sensor -->")

    // -- 开窗  Group Window   行间隔不支持事件时间,只支持处理时间
    // 滚动窗口 Tumble
    val resultGroupTableTumble: Table = sensorTable
      .window(Tumble.over(10.seconds).on('ts).as('tw))
      .groupBy('tw, 'id)
      .select('id, 'id.count as 'num, 'tw.start, 'tw.end)

    // -- 开窗 Over Window
    // 无界数据流
    //    val resultOverTableUnRange: Table = sensorTable.window(Over.partitionBy('id).orderBy('ts).preceding(UNBOUNDED_RANGE).as('tw))
    // 有界数据流
    val resultOverTableRange: Table = sensorTable
      .window(Over.partitionBy('id).orderBy('ts).preceding(10.seconds).as('tw))
      .select('id, 'id.count.over('tw).as('num))

    resultGroupTableTumble.toRetractStream[Row].print("resultGroupTableTumble：")
//    resultOverTableRange.toRetractStream[Row].print("resultOverTableRange：")


    env.execute("FlinkTableWindow")
  }
}
