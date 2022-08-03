package flinkTableApi.connect

import flinkTableApi.bean.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{Elasticsearch, FileSystem, Json, OldCsv, Schema}

/**
 * @Auther: wxf
 * @Date: 2022/8/2 16:59:15
 * @Description: FlinkTableConnectES   输出到 MySql
 * @Version 1.0.0
 */
object FlinkTableConnectMySql {
  def main(args: Array[String]): Unit = {

    // 创建 Flink 流式环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 创建 FlinkTable 流式环境
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance
      .useOldPlanner()    // 新版本 useBlinkPlanner String和Varchar 不兼容
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // 创建 socket 流
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val mapStream: DataStream[SensorReading] = inputStream.map(x => {
      val str: Array[String] = x.split(" ")
      SensorReading(str(0), str(1).toLong, str(2).toDouble)
    })
    val inputTable: Table = tableEnv.fromDataStream(mapStream)
      .filter("id = 'sensor_1'")
      .select("id, timestamp as sj, temperature")

    //    //     读取文件
    //    val path: String = ClassLoader.getSystemResource("sensor1.csv").getPath
    //    println(path)
    //    // 定义数据流 创建表
    //    tableEnv.connect(new FileSystem().path(path))
    //      .withFormat(new OldCsv()) // 定义读取数据之后的格式化方法
    //      .withSchema(new Schema() // 定义表结构
    //        .field("id", DataTypes.STRING())
    //        .field("timestamp", DataTypes.BIGINT())
    //        .field("temperature", DataTypes.DOUBLE())
    //      )
    //      .createTemporaryTable("inputTable") // 注册一张表
    //    // 转换操作
    //    val inputTable: Table = tableEnv.sqlQuery("select * from inputTable where id = 'sensor_1'")


    inputTable.toAppendStream[(String, Long, Double)].print("inputTable: ")

    // 'connector.write.flush.max-rows' = '1'  设置 一条数据写一次 默认是 一次写入 5000条
    val sinkDDL: String =
      """
        |create table jdbcOutputTable (
        |id varchar(20) not null,
        |sj bigint ,
        |temperature Double
        |) with (
        |'connector.type' = 'jdbc',
        |'connector.url' = 'jdbc:mysql://localhost:3306/db',
        |'connector.table' = 'sensor_count',
        |'connector.driver' = 'com.mysql.jdbc.Driver',
        |'connector.username' = 'root',
        |'connector.password' = 'root',
        |'connector.write.flush.max-rows' = '1'
        |)""".stripMargin
    tableEnv.sqlUpdate(sinkDDL)
    inputTable.insertInto("jdbcOutputTable")

    env.execute("FlinkTableConnectMySql")
  }
}
