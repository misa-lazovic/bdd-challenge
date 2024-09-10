package com.bdd.report

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.util.Properties

class ReportGeneratorIntegrationTest extends AnyFunSuite with BeforeAndAfterAll {

  val spark: SparkSession = SparkSession.builder
    .appName("ReportGeneratorIntegrationTest")
    .master("local[*]")
    .getOrCreate()

  val kafkaProps = new Properties()
  kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  val producer = new KafkaProducer[String, String](kafkaProps)

  def sendSampleData(): Unit = {
    val records = Seq(
      """{"view_id":"view1","start_timestamp":"2024-09-09T10:00:00Z","end_timestamp":"2024-09-09T10:01:00Z","banner_id":101,"campaign_id":10}""",
      """{"view_id":"view2","start_timestamp":"2024-09-09T10:05:00Z","end_timestamp":"2024-09-09T10:06:00Z","banner_id":102,"campaign_id":10}"""
    )

    records.foreach(record => {
      producer.send(new ProducerRecord[String, String]("view_log", record))
    })

    producer.flush()
  }

  test("integration test for report generation") {
    sendSampleData()

    ReportGenerator.main(Array.empty)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }
}
