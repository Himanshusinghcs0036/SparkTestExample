package com.poc.test.spark

import java.time.LocalDate

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import scala.util.Random



/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println("Hello World!")
    /*println("concat arguments = " + foo(args))
    val r = Random
    var i = 0
    while (i < 10) {
      val day=10+Math.round(Math.random() * (28 - 10)).toInt
      val month=10+Math.round(Math.random() * (12 - 10)).toInt
      val year=1910+Math.round(Math.random() * (1910 - 2000)).toInt
      println(day+"-"+month+"-"+year)
      i=i+1
    }*/

    val spark = SparkSession.builder().master("local[*]").appName("Sample Spark App").config("spark.sql.warehouse.dir", "/tmp/").getOrCreate()

    val expectedSchema =  StructType(List(StructField("DOB", StringType, true), StructField("Age", IntegerType, true)))
    val expectedData= List(Row("23-11-1999",20),Row("17-10-1960",59))
    val expectedDF=spark.createDataFrame(spark.sparkContext.parallelize(expectedData),expectedSchema)
      .select(to_date(col("DOB"),"dd-MM-yyyy").as("DOB"), col("Age"))
    expectedDF.printSchema()
    expectedDF.show()


  }

}
