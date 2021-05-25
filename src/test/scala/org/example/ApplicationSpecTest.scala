package org.example

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.example.Constants.{CHECK_IN_DATE, HOTEL_ID, IDLE_DAYS, IS_VALID}
import org.junit.Test
import org.scalatest.FunSpec

@Test
class ApplicationSpecTest  extends FunSpec
  with SparkSessionTestWrapper{


  import spark.implicits._

  val application = new Application(spark)

  it("gets bookings"){
    val sourceDF = Seq(
      (1L, "2017-01-01"),
      (1L, "2017-01-10"),
      (2L, "2017-02-11"),
      (2L, "2017-02-12")
    ).toDF(HOTEL_ID, CHECK_IN_DATE)
    val actualDF = sourceDF.transform(application.addIdleDaysToExpedia)

    val expectedSchema = List(
      StructField(HOTEL_ID, LongType, true),
      StructField(CHECK_IN_DATE, StringType, true),
      StructField(IDLE_DAYS, IntegerType, true),
      StructField(IS_VALID, BooleanType, true)
    )

    val expectedData = Seq(
      Row(1L, "2017-01-01", null, false),
      Row(1L, "2017-01-10", 9, false),
      Row(2L, "2017-02-11", null, true),
      Row(2L, "2017-02-12", 1, true)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )
    assert(expectedDF.collectAsList().equals(actualDF.collectAsList()))
  }

  it("gets invalid hotels") {

    val sourceDF = Seq(
      (228L, false),
      (228L, false),
      (25L, true),
      (25L, true),
      (25L, true)
    ).toDF(HOTEL_ID, IS_VALID)

    val actualDF = sourceDF.transform(application.getInvalidHotels)

    val expectedSchema = List(
      StructField(HOTEL_ID, LongType, true),
    )

    val expectedData = Seq(
      Row(228L)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assert(expectedDF.collectAsList().equals(actualDF.collectAsList()))

  }
}
