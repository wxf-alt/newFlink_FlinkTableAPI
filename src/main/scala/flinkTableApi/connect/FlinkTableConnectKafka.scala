package flinkTableApi.connect

import java.util.Properties

import flinkTableApi.bean.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}
import org.apache.flink.types.Row
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * @Auther: wxf
 * @Date: 2022/8/2 11:27:52
 * @Description: FlinkTableConnectKafka   连接外部 kafka,输出到kafka
 * @Version 1.0.0
 */
object FlinkTableConnectKafka {
  def main(args: Array[String]): Unit = {

    // 创建 Flink 流式环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val mapStream: DataStream[SensorReading] = inputStream.map(x => {
      val str: Array[String] = x.split(",")
      SensorReading(str(0), str(1).toLong, str(2).toDouble)
    })

    // 创建 FlinkTable 流式环境
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // kafka 作为输出表
    tableEnv.connect(
      new Kafka()
      .version("0.9")    // 定义 Kafka 的版本
      .topic("FlinkIdeaSinkTest")   // 定义 topic
        .property("zookeeper.connect", "nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181")
        .property("bootstrap.servers", "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092")
    ).withFormat(new Csv()) // 定义读取数据之后的格式化方法
      .withSchema(new Schema() // 定义表结构
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("kafkaOutputTable")

    val table: Table = tableEnv.fromDataStream(mapStream)
    val result: Table = table.filter("id = 'sensor_1'")
      .select("id,timestamp,temperature")

    result.toAppendStream[Row].print("result：")
    result.insertInto("kafkaOutputTable")

    env.execute("FlinkTableConnectKafka")
  }
}
