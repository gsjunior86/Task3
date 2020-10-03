package br.gsj

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SaveMode

object Task3 extends Serializable {

  case class Advertiser_hotels(hotel_id: Int, advertiser: Int)
  case class Clicks(country: String, advertiser: Int, hotel_id: Int, clicks: Int)
  case class Bookings(country: String, advertiser: Int, hotel_id: Int, bookings: Int, booking_volume: Int)

  def main(args: Array[String]): Unit = {

    if (args.length < 1)
      throw new IllegalArgumentException("Usage: scala Task3 <path_where_files_will be_stored>")

    if (!new java.io.File(args(0)).exists)
      throw new IllegalArgumentException("Provided path does not exist")

    val path = args(0)

    val spark = SparkSession.builder()
    .appName("Task3")
    .master("local[*]")
    .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val ah_df = spark.sparkContext.parallelize(
      List(
        Advertiser_hotels(5001, 21),
        Advertiser_hotels(5001, 22),
        Advertiser_hotels(5002, 21))).toDF

    val clicks_df = spark.sparkContext.parallelize(
      List(
        Clicks("US", 21, 5001, 100),
        Clicks("US", 22, 5001, 50),
        Clicks("US", 21, 5002, 10),
        Clicks("IT", 22, 5001, 5))).toDF

    val bookings_df = spark.sparkContext.parallelize(
      List(
        Bookings("US", 21, 5001, 10, 1000),
        Bookings("DE", 21, 5002, 1, 200),
        Bookings("US", 21, 5001, 10, 800),
        Bookings("DE", 21, 5002, 1, 300))).toDF
        
        
    val joined_df = ah_df.join(clicks_df, Seq("hotel_id")).join(bookings_df, Seq("hotel_id"))
       

    //aggregation 1
    clicks_df.coalesce(1).groupBy("hotel_id").agg(sum("clicks").as("total_clicks"))
      .write
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .csv(path + "/task3/aggregations_1")

    //aggregation 2
    bookings_df
    .coalesce(1)
    .groupBy("country", "advertiser")
      .agg(max(col("booking_volume")).as("max_volume"))
      .write
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .csv(path + "/task3/aggregations_2")

    //aggregation 3

    val bookingWindow = Window.partitionBy("advertiser")
      .partitionBy("country").orderBy(col("booking_volume").desc)
    val bookingWindowRank = rank().over(bookingWindow)

    bookings_df.coalesce(1).select($"*", bookingWindowRank as "volume_rank")
      .where($"volume_rank" === 2)
      .write
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .csv(path + "/task3/aggregations_3")

    //aggregation 4
    val ratingUDF = udf((b: Float, c: Float) => b / c)

    joined_df.coalesce(1)
      .groupBy("hotel_id").agg(
        round(
          ratingUDF(
            sum("bookings"),
            sum("clicks")), 2).as("conversion_rate")).write
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .csv(path + "/task3/aggregations_4")

  }

}