//package flinkNew
//
//import java.time.Duration
//
//import org.apache.flink.api.common.ExecutionConfig
//import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.configuration.{ConfigConstants, Configuration}
//import org.apache.flink.connector.kafka.source.KafkaSource
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.util.keys.KeySelectorUtil
//
///**
// * @Auther: wxf
// * @Date: 2022/7/8 14:23:57
// * @Description: FlinkTest   Flink1.14.4版本 设置 Watermarks
// * @Version 1.0.0
// */
//object FlinkTest {
//  def main(args: Array[String]): Unit = {
//    //生成配置对象
//    val conf: Configuration = new Configuration()
//    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
//    // web 查看 http://localhost:8081/
//    //    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
//
//    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//
//    environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
//    environment.getConfig.setAutoWatermarkInterval(100L)
//
//    environment.setParallelism(1)
//
//    //    val kafkaSource: KafkaSource[String] = KafkaSource.builder()
//    //      .setBootstrapServers("")
//    //      .setTopics("input-topic")
//    //      .setGroupId("group.id")
//    //      .setStartingOffsets(OffsetsInitializer.earliest())
//    //      .setValueOnlyDeserializer(new SimpleStringSchema())
//    //      .build()
//    //    val kafakStream: DataStream[String] = environment.fromSource(kafkaSource, WatermarkStrategy.noWatermarks, "Kafka Source")
//    //
//    //    kafakStream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[String](Duration.ofSeconds(100)))
//
//    val inputStream: DataStream[String] = environment.socketTextStream("localhost", 6666)
//
//    val mapStream: DataStream[(String, String, String)] = inputStream.map(x => {
//      val str: Array[String] = x.split(" ")
//      (str(0), str(1), str(2))
//    })
//
//    //    val waterMarkStream: DataStream[(String, String, String)] = mapStream.assignTimestampsAndWatermarks(
//    //      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))[(String, String, String)]
//    //        .withTimestampAssigner(new SerializableTimestampAssigner[(String,String,String)]() {
//    //          override def extractTimestamp(element: (String, String, String), recordTimestamp: Long) = element._2.toLong
//    //        })
//    //      .withIdleness(Duration.ofSeconds(3)))
//
//    val waterMarkStream: DataStream[(String, String, String)] = mapStream.assignAscendingTimestamps(_._2.toLong)
//
//    val windowStream: DataStream[(String, String, String)] = waterMarkStream.keyBy(_._1)
//      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
//      .maxBy(2)
//
//    val characteristic: TimeCharacteristic = environment.getStreamTimeCharacteristic
//    println("characteristic: " + characteristic)
//    windowStream.print("windowStream:")
//
//    environment.execute("FlinkTest")
//  }
//}
