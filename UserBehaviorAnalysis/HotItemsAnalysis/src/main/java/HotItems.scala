
import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
import scala.math.Ordering

/** *
 *
 * @author Zhi-jiang li
 * @date 2020/1/16 0016 17:16
 * */
//输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

//输出数据样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop100:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    //创建一个env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //显示地定义个Time类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 为了打印到控制台的结果不乱序，我们配置全局的并发为1，这里改变并发对结果正确性没有影响
    env.setParallelism(1)

    import org.apache.flink.api.scala._

    val stream = env
      //.readTextFile("H:\\workspace\\ideaDemo\\actual_project\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .addSource(new FlinkKafkaConsumer[String]("hotitems",new SimpleStringSchema(),properties))
      .map(line => {
        val linearray = line.split(",")
        UserBehavior(linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong)
      })
      //指定时间戳和watermark
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv")
      .keyBy("itemId")
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .aggregate(new CountAgg(), new windowResultFunction())
      .keyBy("windowEnd")
      .process(new TopNHotItems(3))
      .print()


    //调用execute执行任务
    env.execute("Hot Item Job")
  }

  //自定义实现聚合函数
  class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  //自定义实现Window Function方法 输出ItemViewCount格式
  class windowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      //先取出itemID(商品ID)
      val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
      //取出统计的数量
      val count: Long = input.iterator.next()
      //收集所有的ItemViewCount
      out.collect(ItemViewCount(itemId, window.getEnd, count))
    }
  }

  //自定义实现process function
  class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {

    //定义一个listState状态
    private var itemState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      // 命名状态变量的名字和状态变量的类型
      val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemState", classOf[ItemViewCount])

      //定义状态变量
      itemState = getRuntimeContext.getListState(itemStateDesc)
    }

    override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      itemState.add(i)
      //根据windowEnd注册定时器,触发时间定为 windowEnd + 1,触发时说明window已经收集完成所有数据
      context.timerService.registerEventTimeTimer(i.windowEnd + 1)
    }

    //定时器触发操作,从State里取出所有数据,排序TopN,输出
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      //获取所有的商品点击信息
      val allItems: ListBuffer[ItemViewCount] = ListBuffer()

      import scala.collection.JavaConversions._

      for (item <- itemState.get()) {
        allItems += item
      }

      //清除状态中的数据,释放内存空间
      itemState.clear()

      //按照点击量从大到小排序, 选取TopN
      val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)


      //将排名数据格式化,便于打印输出
      val result: StringBuilder = new StringBuilder
      result.append("========================================\n")
      result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

      for (i <- sortedItems.indices){
        val currentItem : ItemViewCount = sortedItems(i)
        //输出打印的格式
        result.append("No").append(i+1).append(":")
          .append(" 商品ID=").append(currentItem.itemId)
          .append(" 浏览量=").append(currentItem.count).append("\n")
      }
      result.append("=============================\n\n")

      //控制输出频率
      Thread.sleep(1000)
      out.collect(result.toString())
    }
  }

}
