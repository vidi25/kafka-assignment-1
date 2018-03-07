package edu.knoldus.producer

import java.util.Properties

import edu.knoldus.models.Student
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger

object StudentProducer extends App {

  val log = Logger.getLogger(this.getClass)
  val topic = "student-topic"
  val studentList = List("Aman", "Jatin", "Sakshi", "Soumya", "Raj", "Ajay", "Megha", "Pooja")
  val props = new Properties()

  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "edu.knoldus.serialization.StudentSerializer")

  val producer = new KafkaProducer[String, Student](props)

  log.info("Sending data")
  for (i <- 0 to 7) {
    val key = i.toString
    val value = Student(i, studentList(i))
    val record = new ProducerRecord[String, Student](topic, key, value)
    producer.send(record)
    log.info(s"Data sent...")
  }
  producer.close()
  log.info("Data sending finished")

}
