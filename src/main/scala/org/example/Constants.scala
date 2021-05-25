package org.example

case object Constants {
  val HOTEL_ID = "hotel_id"
  val ID = "id"
  val IDLE_DAYS = "idle_days"
  val IS_VALID = "is_valid"
  val CHECK_IN_DATE = "srch_ci"
  val HOTEL_NAME = "name"
  val HOTEL_ADDRESS = "address"
  val HOTEL_COUNTRY = "country"
  val HOTEL_CITY = "city"
  val LONGITUDE = "longitude"
  val LATITUDE = "latitude"
  val GEOHASH = "geohash"
  val CHECK_IN_YEAR = "ci_year"
  val EXPEDIA_HDFS_PATH_INPUT = "hdfs://localhost:9000/user/hdfs/data/expedia/*.avro"
  val EXPEDIA_HDFS_PATH_OUTPUT = "hdfs://localhost:9000/user/hdfs/data/expedia_valid"
  val HOTELS_KAFKA_TOPIC = "hotels_weather"
}
