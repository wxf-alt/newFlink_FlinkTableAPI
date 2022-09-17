package flinkTableApi.test

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.co.{CoFlatMapFunction, RichCoFlatMapFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/8/31 16:51:32
 * @Description: ConnectionFunctionTest   一个 control控制流是 用来指定哪些词需要从 streamOfWords 里过滤掉的
 * @Version 1.0.0
 */
object ConnectionFunctionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 只有 KeyedStream 才可以使用 键控状态ValueState
    val control: KeyedStream[String, String] = env.fromElements("DROP", "IGNORE").keyBy(x => x)
    val streamOfWords: KeyedStream[String, String] = env.fromElements("Apache", "DROP", "Flink", "IGNORE").keyBy(x => x)

    control.connect(streamOfWords)
      .flatMap(new ControlFunction())
      .print();

    env.execute("ConnectionFunctionTest");
  }
}

class ControlFunction extends RichCoFlatMapFunction[String, String, String] {

  //  var blocked: ValueState[Boolean] = _
  lazy val blocked: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor("blocked", classOf[Boolean]))

  //  override def open(conf: Configuration): Unit = {
  //    // 设置状态
  //    blocked = getRuntimeContext.getState(new ValueStateDescriptor("blocked", classOf[Boolean]))
  //  }

  override def flatMap1(value: String, out: Collector[String]): Unit = blocked.update(true)

  override def flatMap2(value: String, out: Collector[String]): Unit = {
    if (blocked.value() == null) {
      out.collect(value)
    }
  }

}
