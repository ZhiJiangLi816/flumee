import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/** *
 *
 * @author Zhi-jiang li
 * @date 2020/1/21 0021 15:29
 * */
object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val loginStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430832),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.0.3", "fail", 1558430845),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.userId)

    //定义一个匹配模式,next紧邻发生的事件
    val loginFailPatter = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .within(Time.seconds(2))

    //在keyBy之后的流中匹配出定义好的pattern stream
    val patternStream = CEP.pattern(loginStream,loginFailPatter)

    import scala.collection.Map
    //从pattern Stream中获取匹配到的事件流
    val loginFailDataStream = patternStream.select(
      (Pattern: Map[String,Iterable[LoginEvent]])=>{
        val begin = Pattern.getOrElse("begin",null).iterator.next()
        val next = Pattern.getOrElse("next",null).iterator.next()
        (next.userId,begin.ip,next.ip,next.eventTime)
      }
    )
      .print()

    env.execute("Login Fail Detect Job")
  }

}
