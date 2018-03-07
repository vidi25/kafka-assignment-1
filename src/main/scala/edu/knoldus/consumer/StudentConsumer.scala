package edu.knoldus.consumer

import java.util.Properties

import edu.knoldus.models.Student
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.log4j.Logger

import scala.collection.JavaConverters._

object StudentConsumer extends App {

  val log = Logger.getLogger(this.getClass)
  val topic = "student-topic"
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "edu.knoldus.serialization.StudentDeserializer")
  props.put("group.id", "studentTest-group")
  props.put("auto.offset.reset", "earliest")
  props.put("enable.auto.commit", "false")

  val consumer = new KafkaConsumer[String, Student](props)

  consumer.subscribe(java.util.Collections.singletonList(topic))
  while (true) {
    val records = consumer.poll(5000)
    for (record <- records.asScala)
      log.info(s"Student Record:${record.value()}")
  }

}
