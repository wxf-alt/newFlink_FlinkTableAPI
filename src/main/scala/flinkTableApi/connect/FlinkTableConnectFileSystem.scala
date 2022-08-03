package flinkTableApi.connect

import flinkTableApi.bean.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}

/**
 * @Auther: wxf
 * @Date: 2022/8/1 20:53:51
 * @Description: FlinkTableConnectFileSystem   连接外部文件系统/socket 读取csv文件 处理后 输出 csv文件
 * @Version 1.0.0
 */
object FlinkTableConnectFileSystem {
  def main(args: Array[String]): Unit = {

    // flink 流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // flinkTable 流处理环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    val path: String = ClassLoader.getSystemResource("sensor1.csv").getPath
    println(path)

    val inputStream: DataStream[SensorReading] = env.socketTextStream("localhost", 6666)
      .map(x => {
        val str: Array[String] = x.split(" ")
        SensorReading(str(0), str(1).toLong, str(2).toDouble)
      })
    val table: Table = tableEnv.fromDataStream(inputStream)
    tableEnv.createTemporaryView("inputTable",table)

//    // 定义数据流 创建表
//    tableEnv.connect(new FileSystem().path(path))
//      .withFormat(new OldCsv())     // 定义读取数据之后的格式化方法
//      .withSchema(new Schema()      // 定义表结构
//        .field("id",DataTypes.STRING())
//        .field("timestamp",DataTypes.BIGINT())
//        .field("temperature",DataTypes.DOUBLE())
//      )
//      .createTemporaryTable("inputTable")    // 注册一张表


    // 转换操作
    val filterTable: Table = tableEnv.sqlQuery("select * from inputTable where id = 'sensor_1'")

//    // 基于 Table 创建临时视图
//    tableEnv.createTemporaryView("filterView",filterTable)

    val tableStream: DataStream[(String, Long, Double)] = filterTable.toAppendStream[(String, Long, Double)]
    tableStream.print("tableStream:")

    // 定义输出流 sink
    tableEnv.connect(new FileSystem().path("E:\\A_data\\3.code\\newFlink_FlinkTableAPI\\src\\main\\resources\\filterSensorSocket.txt"))
      .inAppendMode()    // 追加的方式输出(在同一个文件中)
      .withFormat(new OldCsv())
      .withSchema(new Schema()      // 定义表结构
        .field("id",DataTypes.STRING())
        .field("timestamp",DataTypes.BIGINT())
        .field("temperature",DataTypes.DOUBLE()))
      .createTemporaryTable("outputTable")
    // 输出
    filterTable.insertInto("outputTable")

    env.execute("FlinkTableConnectFileSystem")

  }
}
