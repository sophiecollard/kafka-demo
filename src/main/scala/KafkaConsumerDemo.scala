import java.util.{Arrays, Properties}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer


object KafkaConsumerDemo {

  def main(args: Array[String]): Unit = {
    var properties = new Properties()

    properties.setProperty("bootstrap.servers", "127.0.0.1:9092")
    properties.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    properties.setProperty("value.deserializer", classOf[StringDeserializer].getName)

    properties.setProperty("group.id", "test")
    properties.setProperty("enable.auto.commit", "true") // offsets are committed automatically
    properties.setProperty("auto.commit.interval.ms", "1000")
    properties.setProperty("auto.offset.reset", "earliest")

    val consumer = new KafkaConsumer[String, String](properties)
    consumer.subscribe(Arrays.asList("first_topic")) // does not work with List[_]

    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(100)
      records.forEach { record =>
        println(s"\ntopic: ${record.topic}")
        println(s"key: ${record.key}")
        println(s"value: ${record.value}")
        println(s"timestamp: ${record.timestamp}")
        println(s"partition: ${record.partition}")
        println(s"offset: ${record.offset}")
      }
    }

    //consumer.commitSync() // triggers offset commit, in case enable.auto.commit was set to false
  }

}