package com.poc.test.spark

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DateType, IntegerType}

/**
  * Provides a Class to Explain BDD Framework with ScalaTest.
  *
  * Main class would instantiate SparkSession and will read data from person file present at resources loc.
  * This Code Will Calculate Age of Person.
  */
object PersonSparkMain {

  def main(args: Array[String]): Unit = {

    implicit val spark:SparkSession=SparkSession.builder().master("local[2]").appName("SPARK_BDD_WithScalaTest").enableHiveSupport().getOrCreate()
    import spark.implicits._
    val dataPath="/Users/hsing110/MyPoc/SparkTDDBDDPOC/src/main/resource/person.csv"
    val dataExtractor= new DataExtractor
    var sourceDF:DataFrame=dataExtractor.getCsvFromHDFS(dataPath,true,",")
    sourceDF=sourceDF.withColumn("DOB", to_date(col("DOB"),"dd-MM-yyyy"))
    val resultDF:DataFrame=getAgeColumn(sourceDF).withColumn("AgeGroup",getAgeGroupUDF(col("Age")))
    resultDF.show()
  }
  def getAgeGroupUDF=udf((age:Int)=>classifyAge(age))

  def getAgeColumn(originalDF:DataFrame):DataFrame={
    originalDF.withColumn("Age", months_between(current_date(), col("DOB")).divide(12).cast(IntegerType))
  }

  /**
    * Method will calculate the age group based on input Age value.
    * Created for UDF Testing
    */
  def classifyAge(age:Int)= age match {
    case it if 0 until 5 contains it => "Baby"
    case it if 6 until 18 contains it => "Child"
    case it if 19 until 30 contains it => "Young"
    case it if 30 until 60 contains it  => "Adult"
    case it if 61 until 110 contains it => "Old"
    case _ => "Invalid Value/Not Considering >110"
  }




}
