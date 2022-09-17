package flinkTableApi.bean

import scala.beans.BeanProperty

/**
 * @Auther: wxf
 * @Date: 2022/7/26 19:50:02
 * @Description: SensorReading
 * @Version 1.0.0
 */
//// 定义样例类，传感器id，时间戳，温度
//// 添加 @BeanProperty 用于 JSON 转换 对象
//case class SensorReading(@BeanProperty id: String, @BeanProperty timestamp: Long, @BeanProperty temperature: Double){
//  override def toString: String = s"id：${id},timestamp：${timestamp}，temperature：${temperature}"
//}

case class SensorReading(id: String, timestamp: Long, temperature: Double) {
  //  override def toString: String = s"id：${id},timestamp：${timestamp}，temperature：${temperature}"
  override def toString: String = s"${id},${timestamp},${temperature}"
}