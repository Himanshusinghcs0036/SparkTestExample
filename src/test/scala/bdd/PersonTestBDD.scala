package bdd

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.poc.test.spark.{DataExtractor, PersonSparkMain}
import org.scalatest.{BeforeAndAfterAll, FunSpec, FunSuite, Matchers}
import sparktest.SparkTestWrapper
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}


class PersonTestBDD extends FunSpec with BeforeAndAfterAll with SparkTestWrapper with Matchers {

  describe("A Person Details should have age column with BDD") {
      it ("should be a Person BDD with Matcher"){
        val verificationDF=PersonSparkMain.getAgeColumn(spark.emptyDataFrame)
        verificationDF.columns should contain ("Age")
    }
  }
   describe("A Person Details should have age column BDD Implementation without Matcher") {
    it ("should be a Person W/O Macher"){
      val verificationDF=PersonSparkMain.getAgeColumn(spark.emptyDataFrame)
      assert(verificationDF.columns.contains("Age"))
    }
  }

  override def afterAll(): Unit = {
    spark.close()
  }

}

class PersonTestTDD_FunSpec extends FunSpec with BeforeAndAfterAll with SparkTestWrapper with Matchers with DataFrameComparer {

  val dataExctractor = new DataExtractor()
  var personDF=dataExctractor.getCsvFromHDFS("/Users/hsing110/MyPoc/SparkTDDBDDPOC/src/test/test_resource/person_test.csv",true,",")
  override def beforeAll(): Unit = {
    super.beforeAll()

  }
  describe("Validate The Age of Based on DOB") {
    it ("Input should be a DataFrame with in Age Column"){
      val expectedSchema =  StructType(List(StructField("DOB", StringType, true), StructField("Age", IntegerType, true)))
      val expectedData= List(Row("23-11-1999",20),Row("17-10-1960",59))
      val expectedDF=spark.createDataFrame(spark.sparkContext.parallelize(expectedData),expectedSchema)
        .select(to_date(col("DOB"),"dd-MM-yyyy").as("DOB"), col("Age"))
      val actualDF=PersonSparkMain.getAgeColumn(personDF.select(to_date(col("DOB"),"dd-MM-yyyy").as("DOB")))
      assertSmallDataFrameEquality(actualDF,expectedDF)
    }
  }

  describe("Test getAgeGroupUDF for testing Spark UDF") {
    it ("Input should contain an Age Column with Integer Value"){
      import spark.implicits._
      val expectedSchema =  StructType(List(StructField("Age", IntegerType, true), StructField("AgeGroup", StringType, true)))
      val expectedData= List(Row(20, "Young"),Row(59, "Adult"))
      val expectedDF=spark.createDataFrame(spark.sparkContext.parallelize(expectedData),expectedSchema)
      val actualSchema =  StructType(List(StructField("Age", IntegerType, true)))
      val actualData= List(Row(20),Row(59))
      var actualDF=spark.createDataFrame(spark.sparkContext.parallelize(actualData),actualSchema)
      actualDF=actualDF.withColumn("AgeGroup", PersonSparkMain.getAgeGroupUDF(col("Age")))
     /* actualDF.show()
      expectedDF.show()*/
      assertSmallDataFrameEquality(actualDF,expectedDF)
    }
  }


  override def afterAll(): Unit = {
    spark.close()
  }

}

class PersonTestTDD_FunSuit extends FunSuite with BeforeAndAfterAll with SparkTestWrapper with Matchers with DataFrameComparer {

  val dataExctractor = new DataExtractor()
  var personDF=dataExctractor.getCsvFromHDFS("/Users/hsing110/MyPoc/SparkTDDBDDPOC/src/test/test_resource/person_test.csv",true,",")
  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  test("Validate value of age column by comparing dataframes"){
    val expectedSchema =  StructType(List(StructField("DOB", StringType, true), StructField("Age", IntegerType, true)))
    val expectedData= List(Row("23-11-1999",20),Row("17-10-1960",59))
    val expectedDF=spark.createDataFrame(spark.sparkContext.parallelize(expectedData),expectedSchema)
      .select(to_date(col("DOB"),"dd-MM-yyyy").as("DOB"), col("Age"))
    val actualDF=PersonSparkMain.getAgeColumn(personDF.select(to_date(col("DOB"),"dd-MM-yyyy").as("DOB")))
    assertSmallDataFrameEquality(actualDF,expectedDF)
  }

  test("Integration Test for Spark Session"){
    val expectedSchema =  StructType(List(StructField("DOB", StringType, true), StructField("Age", IntegerType, true)))
    val expectedData= List(Row("23-11-1999",20),Row("17-10-1960",59))
    val expectedDF=spark.createDataFrame(spark.sparkContext.parallelize(expectedData),expectedSchema)
      .select(to_date(col("DOB"),"dd-MM-yyyy").as("DOB"), col("Age"))
    val actualDF=PersonSparkMain.getAgeColumn(personDF.select(to_date(col("DOB"),"dd-MM-yyyy").as("DOB")))
    assertSmallDataFrameEquality(actualDF,expectedDF)
  }

  override def afterAll(): Unit = {
    spark.close()
  }

}


