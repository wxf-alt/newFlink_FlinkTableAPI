package flinkTableApi.function

import flinkTableApi.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/8/17 19:29:21
 * @Description: ScalarFunctionTest  标量函数
 * @Version 1.0.0
 */
object ScalarFunctionTest {
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

    //    // Table API 调用 自定义函数
    //    val hashCode: HashCode = new HashCode(10)
    //    val resultTable: Table = sensorTable.select('id, 'ts, hashCode('id))
    //
    //    // SQL 调用自定义函数   需要注册函数
    //    tableEnv.registerFunction("hash", new HashCode(10))
    //    tableEnv.createTemporaryView("sensorTable", sensorTable)
    //    val resultSql: Table = tableEnv.sqlQuery("select id,ts,hash(id) from sensorTable")
    //    resultSql.toAppendStream[Row].print("resultTable：")

    val hashCode2: HashCode2 = new HashCode2()
    val resultTable: Table = sensorTable.select('id, 'ts, 'temperature, hashCode2('id), hashCode2('temperature.toString()), hashCode2('id, 'temperature).as('ha))
    resultTable.toAppendStream[Row].print("resultTable：")

    env.execute("ScalarFunctionTest")
  }

  class HashCode(factor: Int) extends ScalarFunction {
    def eval(str: String): Int = {
      str.hashCode * factor
    }
  }

  // 传入多个值 --> 返回一个标量值
  class HashCode2 extends ScalarFunction {

    def eval(str: String): Int = {
      str.hashCode
    }

    def eval(str: Double): Int = {
      str.hashCode
    }

    def eval(id: String, temperature: Double): Int = {
      id.hashCode + temperature.hashCode()
    }
  }

}