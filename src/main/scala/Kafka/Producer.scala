package Kafka

import java.util.Properties

import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}

object Producer {

  def setConfig(config: Config): KafkaProducer[AnyRef, AnyRef] = {
    val props: Properties = new Properties()
    props.put("metadata.broker.list", config.getString("brokerList"))
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("bootstrap.servers", "localhost:9092")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("request.requieres.acks", "1")
    new KafkaProducer(props)
  }

  def send(producer: KafkaProducer[AnyRef, AnyRef], topic: String, message: String): Unit = {
    val record = new ProducerRecord[AnyRef, AnyRef](topic, message)
    producer.send(record)
  }
}
