package com.poc.test.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

class DataExtractor(implicit spark:SparkSession) {

  def getFromHiveTable(db:String, table:String): DataFrame={
    spark.catalog.setCurrentDatabase(db)
    spark.table(table)
  }

  def getCsvFromHDFS(path:String, columnHeader:Boolean, delimeter:String): DataFrame ={
    val readHeader:String = if(columnHeader) "true" else "false"
    spark.read.format("csv").option("header", readHeader).option("delimeter", delimeter).load(path)
  }

}
