package co.com.bvc.kafka

import java.io.FileInputStream
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Promise}


object KafkaProducer {
  val logger = org.apache.log4j.LogManager.getLogger("ETLValorador")

  def sendMessage(topic: String, key: String, message: String) {
    val props: Properties = new Properties
    props.load(new FileInputStream("kafka.properties"))
    val producer = new KafkaProducer[String, String](props)
    val promise = Promise[(RecordMetadata, Exception)]()
    producer.send(new ProducerRecord[String, String](topic, key, message), new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        promise.success((metadata, exception))
      }
    })
    val (metadata, exception) = Await.result(promise.future, new FiniteDuration(3, TimeUnit.SECONDS))
    if (exception == null) logger.info("Message was sent. Offset: " + metadata.offset() + ".Partition: " + metadata.partition() + "Topic: " + metadata.topic())
    else logger.error(exception.toString)
  }
}


  
