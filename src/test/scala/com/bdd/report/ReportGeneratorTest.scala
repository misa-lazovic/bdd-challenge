package com.bdd.report

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class ReportGeneratorTest extends AnyFunSuite with BeforeAndAfterAll {

  val spark: SparkSession = SparkSession.builder
    .appName("ReportGeneratorTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val viewLogData: DataFrame = Seq(
    ("view1", "2024-09-09 10:00:00", "2024-09-09 10:01:00", 101L, 1),
    ("view2", "2024-09-09 10:00:00", "2024-09-09 10:01:00", 102L, 1),
    ("view3", "2024-09-09 10:05:00", "2024-09-09 10:06:00", 103L, 1),
    ("view4", "2024-09-09 10:10:00", "2024-09-09 10:11:00", 104L, 2)
  ).toDF("view_id", "start_timestamp", "end_timestamp", "banner_id", "campaign_id")

  val campaignsData: DataFrame = Seq(
    (1, 1, "Campaign 1"),
    (2, 2, "Campaign 2")
  ).toDF("network_id", "campaign_id", "campaign_name")

  test("report generation logic") {
    val formattedViewLogDF = viewLogData
      .withColumn("start_timestamp", to_timestamp(col("start_timestamp"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("end_timestamp", to_timestamp(col("end_timestamp"), "yyyy-MM-dd HH:mm:ss"))

    val broadcastedCampaigns = broadcast(campaignsData)

    val enrichedViewLogDF = formattedViewLogDF
      .join(broadcastedCampaigns, Seq("campaign_id"), "left")

    val reportDF = enrichedViewLogDF
      .withColumn("duration", unix_timestamp(col("end_timestamp")) - unix_timestamp(col("start_timestamp")))
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

    val result = reportDF.collect()

    assert(result.length == 3, "Expected 3 records in the report")
    assert(result.exists(r => r.getAs[Int]("campaign_id") == 1 && r.getAs[Int]("network_id") == 1), "Expected record for campaign_id 1 and network_id 1")
    assert(result.exists(r => r.getAs[Int]("campaign_id") == 2 && r.getAs[Int]("network_id") == 2), "Expected record for campaign_id 2 and network_id 2")
  }

  override def afterAll(): Unit = {
    spark.stop()
  }
}
