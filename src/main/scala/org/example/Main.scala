package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, datediff, lag}
import org.example.Constants.{CHECK_IN_DATE, HOTEL_ID, IDLE_DAYS}
object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("spark_batching")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()

    val application = new Application(spark)
    val hotels = application.readHotelsFromKafka()
    val expedia = application.readExpediaFromHDFS()

    val expediaWithIdleDays = application.addIdleDaysToExpedia(expedia)
    val invalidHotels = application.getInvalidHotels(expediaWithIdleDays)
    val invalidHotelsInfo = application.getInvalidHotelsInfo(invalidHotels, hotels)
    val validBookings = application.getValidBookings(expediaWithIdleDays)
    val joinedValidBookingsWithHotels = application.getJoinedValidBookingsWithHotels(validBookings, hotels)
    val groupedBookingsByCountry = application.getGroupedBookingsByCountry(joinedValidBookingsWithHotels)
    val groupedBookingsByCity = application.getGroupedBookingsByCity(joinedValidBookingsWithHotels)

    application.showResult(invalidHotelsInfo, groupedBookingsByCountry, groupedBookingsByCity)
    application.writeValidBookingsToHDFS(validBookings)
  }
}
