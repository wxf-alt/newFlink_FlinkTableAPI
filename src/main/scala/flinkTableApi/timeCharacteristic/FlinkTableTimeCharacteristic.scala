package flinkTableApi.timeCharacteristic

import java.time.ZoneId

import flinkTableApi.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Rowtime, Schema}
import org.apache.flink.types.Row

object FlinkTableTimeCharacteristic {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

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
    }) // 设置 waterMark
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading) = element.timestamp * 1000L
      })

    // 方式一：DataStream 转换为 Table 指定时间语义
    //    // 设置 ProcessingTime
    //    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id,'timestamp as 'ts, 'temperature, 'pt.proctime)
    //    // 设置 EventTime   指定字段 或者 新增字段 指定rowtime
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp.rowtime as 'ts, 'temperature)
    //    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp as 'ts, 'temperature, 'rt.rowtime)

    // 方式二：利用 Connect 连接外部系统 设置时间语义
    //    // 设置 ProcessingTime
    //    tableEnv.connect(new FileSystem().path(path))
    //      .withFormat(new Csv().fieldDelimiter(' '))
    //      .withSchema(new Schema()
    //        .field("id", DataTypes.STRING())
    //        .field("timestamp", DataTypes.BIGINT())
    //        .field("temperature", DataTypes.DOUBLE())
    //        // 文件外部系统不支持 添加字段
    //        .field("pt", DataTypes.TIMESTAMP(3)).proctime()
    //      ).createTemporaryTable("sensorTable")
    //    val sensorTable: Table = tableEnv.from("sensorTable")

    //    // 设置 EventTime
    //    tableEnv.connect(new FileSystem().path(path))
    //      .withFormat(new Csv().fieldDelimiter(' '))
    //      .withSchema(new Schema()
    //        .field("id", DataTypes.STRING())
    //        .field("timestamp", DataTypes.BIGINT())
    //        .field("temperature", DataTypes.DOUBLE())
    //        .rowtime(new Rowtime()
    //          .timestampsFromField("timestamp")  // 从字段中提取时间戳
    //          .watermarksPeriodicBounded(1)   // 从字段中提取时间戳
    //        )
    //      ).createTemporaryTable("sensorTable")
    //    val sensorTable: Table = tableEnv.from("sensorTable")


    // 方式三：创建表的 DDL 设置时间语义
    //    // 设置 ProcessingTime
    //    val sinkDDL: String =
    //      s"""create table dataTable (
    //        |  id varchar(20) not null,
    //        |  ts bigint,
    //        |  temperature double,
    //        |  pt AS PROCTIME()
    //        |) with (
    //        |  'connector.type' = 'filesystem',
    //        |  'connector.path' = '${path}',
    //        |  'format.type' = 'csv',
    //        |  'format.field-delimiter' = ' '
    //        |)
    //  """.stripMargin
    //    tableEnv.sqlUpdate(sinkDDL)
    //    val sensorTable: Table = tableEnv.from("dataTable")

    //    // 设置 EventTime
    //    val sinkDDL: String =
    //      s"""create table dataTable (
    //         |  id varchar(20) not null,
    //         |  ts bigint,
    //         |  temperature double,
    //         |  rt AS TO_TIMESTAMP( FROM_UNIXTIME(ts) ),
    //         |  watermark for rt as rt - interval '1' second
    //         |) with (
    //         |  'connector.type' = 'filesystem',
    //         |  'connector.path' = '${path}',
    //         |  'format.type' = 'csv',
    //         |  'format.field-delimiter' = ' '
    //         |)""".stripMargin
    //    tableEnv.sqlUpdate(sinkDDL)
    //    val sensorTable: Table = tableEnv.from("dataTable")


    sensorTable.printSchema()
    sensorTable.toAppendStream[Row].print()

    env.execute("FlinkTableTimeCharacteristic")
  }
}