package com.github.scoquelin.kafka.program

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import tutorial.addressbook.Person
import tutorial.addressbook.Person.{PhoneNumber, PhoneType}

import java.util.Properties

object MainProgram extends App {
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "com.github.scoquelin.kafka.serializers.protobuf.KafkaScalaPBSerializer"
  )
  props.put("schema.registry.url", "http://127.0.0.1:8081")

  val topic = "kafka-test"
  val key = "testkey"

  // test data
  val person = Person()
    .withName("Totoro")
    .withEmail("totoro@tsukamori.jp")
    .withPhones(Seq(PhoneNumber().withNumber("catbus").withType(PhoneType.MOBILE)))

  val producer: Producer[String, Person] = new KafkaProducer[String, Person](props)
  // This is going to register schema automatically by default. Should we?
  // The default behavior of automatically registering schemas can be disabled by passing the property auto.register.schemas=false to the serializer).
  val record: ProducerRecord[String, Person] = new ProducerRecord[String, Person](topic, key, person)
  producer.send(record).get

  producer.close()

}
