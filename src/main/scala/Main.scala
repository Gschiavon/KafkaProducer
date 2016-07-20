import Kafka.Producer
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.KafkaProducer

import scala.io.Source
import scala.util.{Success, Try}
import java.io.File

object Main {

  def main(args: Array[String]): Unit = {

    Try(ConfigFactory.parseFile(new File("/home/gschiavon/desarrollo/stratio/phalanx/KafkaProject/src/main/resources/kafka.conf"))) match {
      case Success(config) =>
        val kafkaProducer =  Producer.setConfig(config)
        readFromFile("/home/gschiavon/desarrollo/stratio/phalanx/KafkaProject/src/main/resources/kafkaproject.txt", kafkaProducer)
    }
  }


  def readFromFile(fileName: String, producer: KafkaProducer[AnyRef, AnyRef] ): Unit = {
    val filename = fileName
    for (line <- Source.fromFile(filename).getLines){
      Producer.send(producer, "testing", line)
      println(line)
    }
  }
}
