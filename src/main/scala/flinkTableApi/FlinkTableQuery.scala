package flinkTableApi

import flinkTableApi.bean.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}

/**
 * @Auther: wxf
 * @Date: 2022/8/3 15:59:03
 * @Description: FlinkTableQuery
 * @Version 1.0.0
 */
object FlinkTableQuery {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // flinkTable 流处理环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    val path: String = ClassLoader.getSystemResource("sensor.txt").getPath
    val inputStream: DataStream[SensorReading] = env.readTextFile(path)
      .map(x => {
        val str: Array[String] = x.split(" ")
        SensorReading(str(0), str(1).toLong, str(2).toDouble)
      })

    val inputTable: Table = tableEnv.fromDataStream(inputStream)
    // 统计每个sensor温度数据出现的个数
    val aggResultTable: Table = inputTable
      .groupBy('id)
      .select('id, 'id.count as 'num)

    //    aggResultTable.toAppendStream[(String,Long)].print("aggResultTable: ")
    // 代码里面出现 group by,count 这种操作时. 必须使用 toRetractStream 转换输出
    aggResultTable.toRetractStream[(String, Long)]
      .filter(_._1).map(_._2)
      .print("aggResultTable：")

    env.execute("FlinkTableQuery")
  }
}
