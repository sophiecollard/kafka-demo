import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


object KafkaProducerDemo {

  def main(args: Array[String]): Unit = {
    var properties = new Properties()

    properties.setProperty("bootstrap.servers", "127.0.0.1:9092")
    properties.setProperty("key.serializer", classOf[StringSerializer].getName)
    properties.setProperty("value.serializer", classOf[StringSerializer].getName)

    properties.setProperty("acks", "1")
    properties.setProperty("retries", "3")
    //properties.setProperty("linger.ms", "1") // alternative to producer.flush, sending values every 1 ms

    val producer: Producer[String, String] = new KafkaProducer[String, String](properties)

    (0 to 10).foreach { n =>
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String]("first_topic", s"$n", s"message $n")
      producer.send(record) // does not actually send the record, only marks it for sending with flush or linger.ms
    }

    producer.flush() // alternative to linger.ms property, sends all records marked for sending

    producer.close()
  }

}
