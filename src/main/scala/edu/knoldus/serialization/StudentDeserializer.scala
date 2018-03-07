package edu.knoldus.serialization

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util

import edu.knoldus.models.Student
import org.apache.kafka.common.serialization.Deserializer

class StudentDeserializer extends Deserializer[Student] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {

  }

  override def deserialize(topic: String, bytes: Array[Byte]) = {
    val byteIn = new ByteArrayInputStream(bytes)
    val objIn = new ObjectInputStream(byteIn)
    val obj = objIn.readObject().asInstanceOf[Student]
    byteIn.close()
    objIn.close()
    obj
  }

  override def close(): Unit = {

  }

}
