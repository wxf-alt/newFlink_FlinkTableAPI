package flinkTableApi.function

import flinkTableApi.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/8/26 15:53:05
 * @Description: AggregateFunctionTest
 * @Version 1.0.0
 */
object AggregateFunctionTest {
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

    //  // Table API -- 第一种调用方式  使用 groupBy + select
    //    val avgTemp: AvgTemp = new AvgTemp()
    //    val avgResult: Table = sensorTable.groupBy('id)
    //      .select('id, avgTemp('temperature) as 'avg_temp)
    //    avgResult.toRetractStream[Row].filter(x => x._1).print("result：")

    //    val maxTemp: MaxTemp = new MaxTemp()
    //    val maxResult: Table = sensorTable.groupBy('id)
    //      .select('id, maxTemp('temperature) as 'max_temp)
    //    maxResult.toRetractStream[Row].filter(x => x._1).print("result：")

    //    // Table API -- 第二种调用方式  使用 groupBy + aggregate
    //    val maxTemp: MaxTemp = new MaxTemp()
    //    val maxResult: Table = sensorTable.groupBy('id)
    //      .aggregate(maxTemp('temperature) as 'max_temp)
    //      .select('id, 'max_temp)
    //    maxResult.toRetractStream[Row].filter(x => x._1).print("result：")

    // Sql 调用
    tableEnv.createTemporaryView("sensorTable",sensorTable)
    tableEnv.registerFunction("AvgTemp",new AvgTemp)
    val resultSqlTable : Table = tableEnv.sqlQuery(
      """
        |select id, AvgTemp(temperature)
        |from sensorTable group by id
        |""".stripMargin)
    resultSqlTable.toRetractStream[Row].filter(x => x._1).print("result：")

    env.execute("AggregateFunctionTest")
  }

  // 计算一下每个sensor的 平均温度值
  class AvgTemp extends AggregateFunction[Double, AvgTempAcc] {
    // 返回最终结果
    override def getValue(accumulator: AvgTempAcc): Double = accumulator.sum / accumulator.count

    // 创建 累加器
    override def createAccumulator(): AvgTempAcc = AvgTempAcc()

    // 对每个输入行调用
    def accumulate(accumulator: AvgTempAcc, temp: Double): Unit = {
      accumulator.count += 1
      accumulator.sum += temp
    }
  }

  // 计算一下每个sensor的 最高温度值
  class MaxTemp extends AggregateFunction[Double, MaxTempAcc] {
    override def getValue(accumulator: MaxTempAcc): Double = accumulator.num

    override def createAccumulator(): MaxTempAcc = MaxTempAcc()

    def accumulate(accumulator: MaxTempAcc, temp: Double): Unit = {
      accumulator.num = Math.max(accumulator.num, temp)
    }
  }

}

// 累加器类型
case class AvgTempAcc() {
  var sum: Double = 0.0
  var count: Int = 0
}

// 累加器类型
case class MaxTempAcc() {
  var num: Double = 0.0
}

