package flinkTableApi.connect

import flinkTableApi.bean.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{Elasticsearch, Json, Schema}

/**
 * @Auther: wxf
 * @Date: 2022/8/2 16:59:15
 * @Description: FlinkTableConnectES   输出到 ES
 * @Version 1.0.0
 */
object FlinkTableConnectES {
  def main(args: Array[String]): Unit = {

    // 创建 Flink 流式环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val mapStream: DataStream[SensorReading] = inputStream.map(x => {
      val str: Array[String] = x.split(" ")
      SensorReading(str(0), str(1).toLong, str(2).toDouble)
    })

    // 创建 FlinkTable 流式环境
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // 创建到 ES 的连接
    tableEnv.connect(
      new Elasticsearch()
        .version("6")
        .host("localhost", 6666, "http")
        .index("sensor")
        .documentType("temp"))
      .inUpsertMode()
      .withFormat(new Json())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("esOutputTable")

    val table: Table = tableEnv.fromDataStream(mapStream)
    table.insertInto("esOutputTable")

    env.execute("FlinkTableConnectES")
  }
}
