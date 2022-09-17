package flinkTableApi.test

import java.util.Properties

import flinkTableApi.bean.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

import scala.util.Random

/**
 * @Auther: wxf
 * @Date: 2022/8/29 20:59:59
 * @Description: KafkaSinkDemo   自定义 Source 输出到 Kafka
 * @Version 1.0.0
 */
object KafkaSinkDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val customSource: DataStream[String] = env.addSource(MyFlinkSourceFunction())
    val mapStream: DataStream[String] = customSource.map(s => {
      val str: Array[String] = s.split("-")
      SensorReading(str(0), str(1).toLong, str(2).toDouble).toString
    })

    // kafka Producer 配置
    val properties: Properties = new Properties()
    properties.put("bootstrap.servers", "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092")
    mapStream.addSink(new FlinkKafkaProducer011[String]("FlinkIdeaSinkTest", new SimpleStringSchema(), properties)).setParallelism(3)

    mapStream.print("mapStream：").setParallelism(1)
    env.execute("KafkaSourceSink")

  }
}

case class MyFlinkSourceFunction() extends SourceFunction[String] {

  private var cancelFlag: Boolean = false

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val random: Random = new Random()
    while (!cancelFlag) {
      // 获取 1-10 的随机数
      val id: Int = random.nextInt(10) + 1
      val temp: String = (random.nextDouble() * 99 + 1).formatted("%.2f") // 保留2位小数
      val timeMillis: Long = System.currentTimeMillis()
      Thread.sleep(id * 200)
      ctx.collect(s"sensor_${id}" + "-" + timeMillis + "-" + temp)
    }
  }

  override def cancel(): Unit = cancelFlag = true
}
