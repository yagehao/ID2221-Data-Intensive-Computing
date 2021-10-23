package generator

import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import kafka.producer.KeyedMessage
import scala.io.Source

object ScalaProducerExample extends App {


    val fileName = "data.csv"
    val events = 110000
    val topic = "covid"
    val brokers = "localhost:9092"

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "ScalaProducerExample")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  
    val producer = new KafkaProducer[String, String](props)

    for(line<-Source.fromFile(fileName).getLines().drop(1)){
	    val key = line.split(","){8} 
        val data = new ProducerRecord[String, String](topic, key, line)
        producer.send(data)
        print(data + "\n")
   
	}
    producer.close()
}
