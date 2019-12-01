package sparktest

import org.apache.spark.sql.SparkSession

trait SparkTestWrapper {

  lazy implicit val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark test POC")
      .config("spark.sql.shuffle.partitions", "1")
      .enableHiveSupport()
      .getOrCreate()
  }

}
