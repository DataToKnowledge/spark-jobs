package it.dtk

import java.util.Properties

import com.google.common.base.Charsets
import org.apache.kafka.clients.producer.{RecordMetadata, Callback, ProducerRecord, KafkaProducer}

/**
  * Created by fabiofumarola on 21/02/16.
  */
object KafkaUtils {

  /**
    *
    * @param kafkaServer A list of servers in the form host1:port1,host2:port2,...
    * @param cliendId
    * @return
    */
  def kafkaWriter(kafkaServer: String, cliendId: String): KafkaProducer[Array[Byte], Array[Byte]] = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaServer)
    //    props.put("compression.type", "snappy")
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    new KafkaProducer[Array[Byte], Array[Byte]](props)
  }

  def producerRecord(topic: String, partition: Option[Int] = None, key: String, value: String): ProducerRecord[Array[Byte], Array[Byte]] = {
    if (partition.isDefined)
      new ProducerRecord[Array[Byte], Array[Byte]](topic, partition.get, key.getBytes, value.getBytes)
    else new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes, value.getBytes)
  }
}


object Main extends App {

  val writer = KafkaUtils.kafkaWriter("192.168.99.100:9092","")

  val record = KafkaUtils.producerRecord("test", None, "ciao", "ciao")
  (1 to 10).foreach{ i =>
    val data = writer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        println(metadata)
        println(exception)
      }
    }).get()

    println(data)
  }

}