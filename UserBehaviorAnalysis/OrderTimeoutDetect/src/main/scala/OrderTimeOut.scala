import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/** *
 *
 * @author Zhi-jiang li
 * @date 2020/1/22 0022 20:17
 **/

//定义输入的订单事件流
case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)

//定义输出的结果
case class OrderResult(orderId: Long, eventType: String)

object OrderTimeOut {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val orderEventStream = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(2, "pay", 1558430844)
    ))
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.orderId)

    //定义Pattern
    val orderPayPattern = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .followedBy("follow")
      .where(_.eventType == "pay")
      .within(Time.minutes(15))

    //定义一个输出标签,用于标明侧输出流
    val orderTimeoutOutput = OutputTag[OrderResult]("orderTimeout")

    //从keyBy之后的每条流中匹配定义好的模式,得到一个pattern stream
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    import scala.collection.Map
    //从Pattern stream中获得输出流
    val completeResult = patternStream.select(orderTimeoutOutput)(
      //对于超时的序列部分,调用PatternTimeoutFunction
      (pattern: Map[String, Iterable[OrderEvent]], timeStamp: Long) => {
        val timeOutOrderId = pattern.getOrElse("begin", null).iterator.next().orderId
        OrderResult(timeOutOrderId, "timeout")
      }
    )(
      //正常匹配的部分,调用Pattern select function
      (pattern: Map[String, Iterable[OrderEvent]]) => {
        val payOrderId = pattern.getOrElse("follow", null).iterator.next().orderId
        OrderResult(payOrderId, "success")
      }
    )
      //打印出的是匹配的时间序列
    completeResult.print()
    //打印输出timeout结果
    val timeOutResultDataStream = completeResult.getSideOutput(orderTimeoutOutput)
    timeOutResultDataStream.print()

    env.execute("Order TimeOut Detect Job")
  }
}
