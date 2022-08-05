package flinkTableApi.timeCharacteristic

import java.sql.Timestamp
import java.time.ZoneId

import flinkTableApi.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Rowtime, Schema}

/**
 * @Auther: wxf
 * @Date: 2022/8/4 19:15:10
 * @Description: FlinkTableEventTime  设置事件时间
 * @Version 1.0.0
 */
object FlinkTableEventTime {
  def main(args: Array[String]): Unit = {

    // 创建 流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) // 设置全局并行度 1
    // 设置 时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 创建流处理 Table环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)
    tableEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    val path: String = ClassLoader.getSystemResource("sensor.txt").getPath
    val inputStream: DataStream[SensorReading] = env.readTextFile(path)
      .map(x => {
        val str: Array[String] = x.split(" ")
        SensorReading(str(0), str(1).toLong, str(2).toDouble)
      }).assignAscendingTimestamps(_.timestamp * 1000L) // 指定时间戳字段 和 分配 waterMark

    //    // 1)DataStream转化成Table时指定
    //    //    在DataStream转换成Table，schema的定义期间，使用.rowtime可以定义事件时间属性
    //    //    注意：必须在转换的数据流中分配时间戳和watermark。
    //    // 将 DataStream转换为 Table，并指定时间字段
    //    //    val sensorTable: Table = tableEnv.fromDataStream(inputStream, 'id, 'timestamp.rowtime, 'temperature)
    //    // 或者，直接追加字段
    //    val sensorTable: Table = tableEnv.fromDataStream(inputStream, 'id, 'timestamp, 'temperature, 'rt.rowtime)
    //    sensorTable.toAppendStream[(String, Long, Double, Timestamp)].print("sensorTable:")

    //    // 2) 定义Table Schema时指定  指定哪个字段作为 EventTime
    //    tableEnv.connect(new FileSystem().path(path))
    //      .withFormat(new Csv().fieldDelimiter(' '))
    //      .withSchema(new Schema()
    //        .field("id", DataTypes.STRING())
    //        .field("timeStamp", DataTypes.BIGINT())
    //        .rowtime(new Rowtime()
    //          .timestampsFromField("timestamp") // 从字段中提取时间戳
    //          .watermarksPeriodicBounded(1000) // watermark延迟1秒
    //        )
    //        .field("temperature", DataTypes.DOUBLE())
    //      ).createTemporaryTable("sensor")
    //        val sensorTable: Table = tableEnv.sqlQuery("select * from sensor")
    //        sensorTable.toAppendStream[(String, Long, Double)].print("sensorTable:")

    // 3)创建表的DDL中指定
    val sourceDDL: String =
      s"""
         |create table sensor (
         | id varchar(20) not null,
         | ts bigint,
         | temperature double,
         | rt AS TO_TIMESTAMP( FROM_UNIXTIME(ts) ),
         | watermark for rt as rt - interval '1' second
         | )with(
         | 'connector.type' = 'filesystem',
         | 'connector.path' = 'file:///E:\\A_data\\3.code\\newFlink_FlinkTableAPI\\src\\main\\resources\\sensor1.csv',
         | 'format.type' = 'csv'
         | )
         |""".stripMargin
    tableEnv.sqlUpdate(sourceDDL)
    val sensorTable: Table = tableEnv.sqlQuery("select * from sensor")
    sensorTable.toAppendStream[(String, Long, Double,Timestamp)].print("sensorTable:")

    env.execute("FlinkTableEventTime")
  }
}
