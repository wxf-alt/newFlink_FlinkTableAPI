package flinkTableApi.function

import flinkTableApi.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/8/26 17:35:39
 * @Description: TableAggregateFunctionTest  提取每个sensor最高的两个温度值
 * @Version 1.0.0
 */
object TableAggregateFunctionTest {
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

    // Table API -- 第一种调用方式  使用 groupBy + select
    val top2Temp: Top2Temp = new Top2Temp()
    val top2Result: Table = sensorTable.groupBy('id)
      .flatAggregate(top2Temp('temperature) as('temp, 'rank))
      .select('id, 'temp, 'rank)
    top2Result.toRetractStream[Row].filter(x => x._1).print("result：")

    env.execute("TableAggregateFunctionTest")
  }

  // 提取每个sensor最高的两个温度值
  class Top2Temp extends TableAggregateFunction[(Double, Int), Top2TempAcc] {

    override def createAccumulator(): Top2TempAcc = Top2TempAcc()

    def accumulate(acc: Top2TempAcc, temp: Double): Unit = {
      if (acc.highestTemp < temp) {
        acc.secondHighestTemp = acc.highestTemp
        acc.highestTemp = temp
      } else if (acc.secondHighestTemp < temp) {
        acc.secondHighestTemp = temp
      }
    }

    def emitValue(acc: Top2TempAcc, out: Collector[(Double, Int)]): Unit = {
      out.collect(acc.highestTemp, 1)
      out.collect(acc.secondHighestTemp, 2)
    }

  }

}

case class Top2TempAcc() {
  var highestTemp: Double = Int.MinValue
  var secondHighestTemp: Double = Int.MinValue
}
