package com.bdd.report

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object ReportGenerator {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("Report Generator")
      .master("local[*]")
      .getOrCreate()

    // Define the schema for the view_log
    val viewLogSchema: StructType = StructType(Array(
      StructField("view_id", StringType),
      StructField("start_timestamp", TimestampType),
      StructField("end_timestamp", TimestampType),
      StructField("banner_id", LongType),
      StructField("campaign_id", IntegerType)
    ))

    // Load static campaigns.csv data
    val campaignsDF: DataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/app/storage/campaigns.csv")

    // Broadcast campaigns
    val broadcastedCampaigns: Dataset[Row] = broadcast(campaignsDF)

    // Create a streaming DataFrame from Kafka
    val kafkaStream: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9093")
      .option("subscribe", "view_log")
      .load()

    // Parse the Kafka message value as a view log
    val viewLogDF: DataFrame = kafkaStream.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), viewLogSchema).as("data")).select("data.*")

    // Join view log data with campaigns data
    val enrichedViewLogDF: DataFrame = viewLogDF
      .join(broadcastedCampaigns, Seq("campaign_id"), "left")

    val reportDF: DataFrame = enrichedViewLogDF
      .withColumn("duration", unix_timestamp(col("end_timestamp")) - unix_timestamp(col("start_timestamp")))
      .withWatermark("end_timestamp", "1 minute") // Adding watermark
      .groupBy(
        window(col("end_timestamp"), "1 minute"),
        col("campaign_id"),
        col("network_id")
      )
      .agg(
        avg("duration").as("avg_duration"),
        count("*").as("total_count")
      )
      .withColumn("minute_timestamp", col("window.start"))
      .drop("window")

    val query = reportDF
      .writeStream
      .format("parquet")
      .option("checkpointLocation", "/app/storage/checkpoint")
      .option("path", "/app/storage/output")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}
