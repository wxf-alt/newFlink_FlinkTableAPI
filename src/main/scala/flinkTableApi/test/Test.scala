package flinkTableApi.test

import java.time.{LocalDate, LocalDateTime}

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeFilter
import flinkTableApi.bean.SensorReading
import org.json4s.jackson.{JsonMethods, Serialization}
import scala.util.Random

/**
 * @Auther: wxf
 * @Date: 2022/8/30 09:11:11
 * @Description: Test
 * @Version 1.0.0
 */
object Test {
  def main(args: Array[String]): Unit = {

    // 随机数
    val random: Random = new Random()
    val i: Int = random.nextInt(10) + 1
    println(i)

    // 随机数 double
    val temp1: String = (random.nextDouble() * 100 + 1).formatted("%06.3f") // 整个输出保留6位数长度，且保留3位小数
    val temp2: String = (random.nextDouble() * 100 + 1).formatted("%.2f") // 保留2位小数
    println(temp1)

//    // 对象 转换 Json   使用 fastjson工具实现Json转换
//    val reading: SensorReading = new SensorReading("sensor_1", 1547718199000L, 35.8)
//    val str: String = JSON.toJSONString(reading, null.asInstanceOf[Array[SerializeFilter]])
//    println(str)
//
//    val reading1: SensorReading = JSON.parseObject(str, classOf[SensorReading])
//    println(reading1)

    // scala 自带 Json4s工具
    val reading: SensorReading = new SensorReading("sensor_1", 1547718199000L, 35.8)
    // 隐式转换
    implicit val f = org.json4s.DefaultFormats
    // 对象转换字符串
    val str: String = Serialization.write(reading)
    println(str)

    // 字符串 转换 对象
    val reading1: SensorReading = JsonMethods.parse(str).extract[SensorReading]
    println(reading1)


  }
}
