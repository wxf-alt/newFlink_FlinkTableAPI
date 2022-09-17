package flinkTableApi.test

import java.util.Properties

import flinkTableApi.bean.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.state.StateTtlConfig.{StateVisibility, UpdateType}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * @Auther: wxf
 * @Date: 2022/8/2 11:27:52
 * @Description: FlinkTableConnectKafka   连接外部 kafka 读取kafka数据
 * @Version 1.0.0
 */
object KafkaSourceDemo {
  def main(args: Array[String]): Unit = {

    // 创建 Flink 流式环境
    //    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val conf: Configuration = new Configuration()
    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    // web 查看 http://localhost:8081/
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(8)

    // 设置 checkPoint
    env.enableCheckpointing(1000)
    env.setStateBackend(new FsStateBackend("file:///E:\\A_data\\4.测试数据\\flink-checkPoint\\KafkaSourceDemo"))

    // 读取 Kafka
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092")
    properties.setProperty("group.id", "idea-consumer-group1")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "earliest")
    val flinkKafkaConsumer011: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String]("FlinkIdeaSinkTest", new SimpleStringSchema(), properties)
    // 设置根据程序checkpoint进行offset提交
    flinkKafkaConsumer011.setCommitOffsetsOnCheckpoints(true)
    // 从提交的 offset 位置开始读取数据
    flinkKafkaConsumer011.setStartFromGroupOffsets()
    val kafkaInputStream: DataStream[String] = env.addSource(flinkKafkaConsumer011)/*.setParallelism(3)*/
    kafkaInputStream.print("kafkaInputStream：")

    val mapStream: DataStream[SensorReading] = kafkaInputStream.map(x => {
      val str: Array[String] = x.split(",")
      // 模拟 复杂处理
      val time: Int = new Random().nextInt(10) + 1
      Thread.sleep(time * 200)
      SensorReading(str(0), str(1).toLong * 1000, str(2).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](org.apache.flink.streaming.api.windowing.time.Time.seconds(5)) {
      override def extractTimestamp(element: SensorReading) = element.timestamp
    })
    //    mapStream.print("mapStream：").setParallelism(1)

    val keyStream: KeyedStream[SensorReading, String] = mapStream.keyBy(_.id)
    val flatMapStream: DataStream[(String, Long, Double)] = keyStream.flatMap(new CountWindowAverageTtl())

    flatMapStream.print("flatMapStream：")
    env.execute("KafkaSourceDemo")
  }
}

class CountWindowAverageTtl extends RichFlatMapFunction[SensorReading, (String, Long, Double)] {

  // 创建 值状态
  var sum: ValueState[(Long, Double)] = _

  override def open(parameters: Configuration): Unit = {
    val valueStateDescriptor: ValueStateDescriptor[(Long, Double)] = new ValueStateDescriptor[(Long, Double)]("average", createTypeInformation[(Long, Double)])
    val stateTtlConfig: StateTtlConfig = StateTtlConfig
      .newBuilder(Time.minutes(2))
      .setUpdateType(UpdateType.OnCreateAndWrite) // 在创建和写入时更新(默认)
      .setStateVisibility(StateVisibility.NeverReturnExpired) // 从不返回过期数据(默认)
      .disableCleanupInBackground() // 关闭后台清理 在后台禁用过期状态的默认清理(默认启用)
      .cleanupIncrementally(10, true)
      .build()
    valueStateDescriptor.enableTimeToLive(stateTtlConfig)
    sum = getRuntimeContext.getState(valueStateDescriptor)

  }

  override def flatMap(value: SensorReading, out: Collector[(String, Long, Double)]): Unit = {
    // 获取状态
    val tmpCurrentSum: (Long, Double) = sum.value()

    // 第一次使用，赋初始值
    val currentSum: (Long, Double) = if (tmpCurrentSum != null) {
      tmpCurrentSum
    } else {
      (0L, 0D)
    }

    // 进来一条数据 更新状态   不论后面什么逻辑,只要进来一条数据 就更新状态
    val newSum: (Long, Double) = (currentSum._1 + 1, currentSum._2 + value.temperature)
    sum.update(newSum)
    //    println(sum.value()._1 + "-----" + sum.value()._2)

    // 如果计数达到10，则发出平均值并清除状态
    // 状态是 实时更新的 下面的 newSum 也可以使用 sum状态 进行计算。
    //    因为上面已经将newSum的值更新到状态 sum 中
    if (newSum._1 >= 2) {
      out.collect((value.id, newSum._1, newSum._2 / newSum._1))
      sum.clear()
    }
  }

}