import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/** *
 *
 * @author Zhi-jiang li
 * @date 2020/1/19 0019 14:06
 * */
object KafkaProducer {
  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }

  def writeToKafka(topic:String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers","hadoop100:9092")
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String,String](props)
    val bufferedSource = io.Source.fromFile("H:\\workspace\\ideaDemo\\actual_project\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    for (line <- bufferedSource.getLines()){
      val record = new ProducerRecord[String,String](topic,line)
      producer.send(record)
    }
    producer.close()
  }
}
