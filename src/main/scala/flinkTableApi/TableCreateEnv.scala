package flinkTableApi

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}

/**
 * @Auther: wxf
 * @Date: 2022/8/1 20:31:11
 * @Description: TableCreateEnv  创建FlinkTable 执行环境
 * @Version 1.0.0
 */
object TableCreateEnv {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    println("创建 流处理模式 表环境")
    //    // 1.简单版本
    //    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //    // 2.创建老版本 planner计划器 执行环境
    //    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
    //      .useOldPlanner // 使用老版本
    //      .inStreamingMode() // 使用流处理模式
    //      .build()
    //    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // 3.创建 blink 版本的流查询环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)


    println("创建 批处理模式 表环境")
    // 1.创建老版本的批式查询环境
    val batch: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    BatchTableEnvironment.create(batch)

    // 2.创建blink版本的批式查询环境
    val bbSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val bbTableEnv: TableEnvironment = TableEnvironment.create(bbSettings)


  }
}
