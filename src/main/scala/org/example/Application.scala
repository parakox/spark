package org.example


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.example.Constants.{CHECK_IN_DATE, CHECK_IN_YEAR, EXPEDIA_HDFS_PATH_INPUT, EXPEDIA_HDFS_PATH_OUTPUT, GEOHASH, HOTELS_KAFKA_TOPIC, HOTEL_ADDRESS, HOTEL_CITY, HOTEL_COUNTRY, HOTEL_ID, HOTEL_NAME, ID, IDLE_DAYS, IS_VALID, LATITUDE, LONGITUDE}


class Application(spark : SparkSession) {

  import spark.implicits._

  def readHotelsFromKafka() : DataFrame = {
    spark.read.json(spark
    .read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:31090")
    .option("subscribe", HOTELS_KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load
    .select($"value".cast(StringType))
    .as[String])
    .dropDuplicates(ID)
    .select(ID, HOTEL_NAME, HOTEL_COUNTRY, HOTEL_CITY, HOTEL_ADDRESS, LONGITUDE, LATITUDE, GEOHASH)
  }

  def readExpediaFromHDFS() : DataFrame = {
    spark
      .read
      .format("avro")
      .load(EXPEDIA_HDFS_PATH_INPUT)
  }

  def addIdleDaysToExpedia(expedia : DataFrame) : DataFrame = {
    val idleDays = datediff(col(CHECK_IN_DATE), lag(CHECK_IN_DATE, 1).over(Window.partitionBy(HOTEL_ID).orderBy(CHECK_IN_DATE)))
    val isValid = min(!col(IDLE_DAYS).between(2,30)).over(Window.partitionBy(HOTEL_ID))
    expedia
      .withColumn(IDLE_DAYS, idleDays)
      .withColumn(IS_VALID, isValid)
  }

  def getInvalidHotels(expediaWithIdleDays : DataFrame) : DataFrame = {
    expediaWithIdleDays
      .filter(!col(IS_VALID))
      .dropDuplicates(HOTEL_ID)
      .select(HOTEL_ID)
  }

  def getInvalidHotelsInfo(invalidHotels : DataFrame, hotels : DataFrame) : DataFrame = {
    invalidHotels
      .join(hotels, invalidHotels.col(HOTEL_ID) === hotels.col(ID))
      .select(ID, HOTEL_NAME, HOTEL_COUNTRY, HOTEL_CITY, HOTEL_ADDRESS, LONGITUDE, LATITUDE, GEOHASH)
  }

  def getValidBookings(expediaWithIdleDays : DataFrame) : DataFrame = {
    expediaWithIdleDays
      .filter(col(IS_VALID))
  }

  def getJoinedValidBookingsWithHotels(validBookings : DataFrame, hotels : DataFrame) : DataFrame = {
    validBookings
      .join(hotels, validBookings.col(HOTEL_ID) === hotels.col(ID))
      .select(HOTEL_COUNTRY, HOTEL_CITY)
  }

  def getGroupedBookingsByCountry(joinedValidBookingsWithHotels : DataFrame) : DataFrame = {
    joinedValidBookingsWithHotels
      .groupBy(HOTEL_COUNTRY)
      .count()
  }

  def getGroupedBookingsByCity(joinedValidBookingsWithHotels : DataFrame) : DataFrame = {
    joinedValidBookingsWithHotels
      .groupBy(HOTEL_CITY)
      .count()
  }

  def showResult(invalidHotelsInfo : DataFrame, groupedBookingsByCountry : DataFrame, groupedBookingsByCity : DataFrame) : Unit = {
    invalidHotelsInfo.show()
    groupedBookingsByCountry.show()
    groupedBookingsByCity.show()
  }

  def writeValidBookingsToHDFS(validBookings : DataFrame) : Unit = {
    validBookings
      .drop(IDLE_DAYS)
      .drop(IS_VALID)
      .withColumn(CHECK_IN_YEAR, year(col(CHECK_IN_DATE)))
      .write.mode(SaveMode.Overwrite)
      .partitionBy(CHECK_IN_YEAR)
      .parquet(EXPEDIA_HDFS_PATH_OUTPUT)
  }

}
