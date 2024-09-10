package com.bdd.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import java.time.Instant
import java.util.Properties
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

object Producer {

  def main(args: Array[String]): Unit = {
    val producer = createProducer()

    def generateViewLog(): String = {
      val viewId = Random.nextInt(10000).toString
      val startTimestamp = Instant.now().minusSeconds(Random.nextInt(300)).toString
      val endTimestamp = Instant.now().toString
      val bannerId = Random.nextInt(10000)
      val campaignId = (Random.nextInt(13) + 1) * 10

      s"""{
         |  "view_id": "$viewId",
         |  "start_timestamp": "$startTimestamp",
         |  "end_timestamp": "$endTimestamp",
         |  "banner_id": $bannerId,
         |  "campaign_id": $campaignId
         |}""".stripMargin
    }

    while (true) {
      val viewLog = generateViewLog()
      val record = new ProducerRecord[String, String]("view_log", viewLog)

      Future {
        producer.send(record, (metadata: RecordMetadata, exception: Exception) => {
          if (exception != null) {
            println(s"Error sending record: ${exception.getMessage}")
          } else {
            println(s"Sent record with offset ${metadata.offset()} to partition ${metadata.partition()}")
          }
        }
        )
      }

      Thread.sleep(1000)
    }

    producer.close()
  }

  private def createProducer(): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    new KafkaProducer[String, String](props)
  }
}
