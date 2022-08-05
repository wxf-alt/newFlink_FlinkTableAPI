package flinkTableApi.timeCharacteristic

import java.sql.Timestamp
import java.time.ZoneId

import flinkTableApi.bean.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * @Auther: wxf
 * @Date: 2022/8/4 14:22:41
 * @Description: FlinkTableTimeCharacteristic   时间语义
 * @Version 1.0.0
 */
object FlinkTableProcessTime {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 创建表执行环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)
    // 设置 时区
    tableEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    val path: String = ClassLoader.getSystemResource("sensor.txt").getPath
    val inputStream: DataStream[String] = env.readTextFile(path)
    val dataStream: DataStream[SensorReading] = inputStream.map(x => {
      val str: Array[String] = x.split(" ")
      SensorReading(str(0), str(1).toLong, str(2).toDouble)
    })

    // 定义处理时间属性有三种方法：   ProcessingTime,EventTime
    //    1. 在DataStream转化时直接指定；
    //    2. 在定义Table Schema时指定；
    //    3. 在创建表的DDL中指定;

//    // 1.DataStream转化成Table时指定proctime;
//    // 注意：这个proctime属性只能通过附加逻辑字段，来扩展物理schema。因此，只能在schema定义的末尾定义它。
//    val sensorTableProcessingTime: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp, 'temperature, 'pt.proctime)
//    sensorTableProcessingTime.toAppendStream[(String,Long,Double,Timestamp)].print("sensorTableProcessingTime:")

    // 2.定义Table Schema时指定：在定义Schema的时候，加上一个新的字段，并指定成proctime就可以了。
//    tableEnv.connect(new FileSystem().path("E:\\A_data\\3.code\\newFlink_FlinkTableAPI\\src\\main\\resources\\SensorTime.txt"))
    tableEnv.connect(new FileSystem().path(path))
      .withFormat(new Csv().fieldDelimiter(' '))
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
//        .field("pt", DataTypes.TIMESTAMP(3)).proctime() // 指定 pt字段为处理时间   有问题
      ).createTemporaryTable("inputTable")
    tableEnv.sqlQuery("select *,LOCALTIMESTAMP  as pt from inputTable")
      .toAppendStream[(String,Long,Double,Timestamp)].print("inputTable:")

//    // 3.创建表的DDL中指定：在创建表的DDL中，增加一个字段并指定成proctime，也可以指定当前的时间字段
//    val sinkDDL: String =
//      s"""
//        |create table inputTable(
//        | id varchar(20) not null,
//        | ts bigint,
//        | temperature double,
//        | pt AS PROCTIME()
//        |)with(
//        |  'connector.type' = 'filesystem',
//        |  'connector.path' = 'file:///E:\\A_data\\3.code\\newFlink_FlinkTableAPI\\src\\main\\resources\\sensor.txt',
//        |  'format.type' = 'csv',
//        |  'format.field-delimiter' = ' '
//        |)
//        |""".stripMargin
//    tableEnv.sqlUpdate(sinkDDL) // 执行 DDL
//    tableEnv.sqlQuery("select * from inputTable").toAppendStream[(String,Long,Double,Timestamp)].print("inputTable:")

    env.execute("FlinkTableTimeCharacteristic")
  }
}
