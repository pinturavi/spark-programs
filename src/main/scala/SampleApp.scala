import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{Column, SparkSession, functions}

object SampleApp {

  final val OUTPUT_FILE_PATH = "output.xlsx"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("SampleApp")
      .master("local[4]")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection")
      .getOrCreate()
    val ds = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("file:/home/ubuntu/spark_data/311data.csv")
    val filteredDuplicateDS = ds.filter("DUPLICATE = true").groupBy(ds("SR_TYPE")).count//.select(ds("CITY"))
    val filteredStattusDS = ds.groupBy(ds("STATUS")).count
    val filteredCityDS = ds.groupBy(ds("CITY")).count
    val filteredDS = ds.withColumn("CREATED", new Column(ds("CREATED_DATE").toString.split(" ")(0)))
    val filteredCreatedDateDS = filteredDS.groupBy(filteredDS("STATUS"), filteredDS("CREATED")).count
    filteredCreatedDateDS.show()
   // MongoSpark.save(filteredDS.write.option("collection", "spark-1").mode("overwrite").format("com.mongodb.spark.sql"))
    filteredDuplicateDS.write
      .format("com.crealytics.spark.excel")
      .option("dataAddress", "'Duplicate_Count[#All]")
      .option("useHeader", "true")
      .option("dateFormat", "yy-mmm-d") // Optional, default: yy-m-d h:mm
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss.000
      .mode("append") // Optional, default: overwrite.
      .save(OUTPUT_FILE_PATH)
    filteredStattusDS.write
      .format("com.crealytics.spark.excel")
      .option("dataAddress", "'Status_Count'!B3:C35")
      .option("useHeader", "true")
      .option("dateFormat", "yy-mmm-d") // Optional, default: yy-m-d h:mm
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss.000
      .mode("append") // Optional, default: overwrite.
      .save(OUTPUT_FILE_PATH)
    filteredCityDS.write
      .format("com.crealytics.spark.excel")
      .option("dataAddress", "'City_Count'!B3:C35")
      .option("useHeader", "true")
      .option("dateFormat", "yy-mmm-d") // Optional, default: yy-m-d h:mm
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss.000
      .mode("append") // Optional, default: overwrite.
      .save(OUTPUT_FILE_PATH)
    filteredCreatedDateDS.write
      .format("com.crealytics.spark.excel")
      .option("dataAddress", "'CreatedDate_Count'!B3:C35")
      .option("useHeader", "true")
      .option("dateFormat", "yy-mmm-d") // Optional, default: yy-m-d h:mm
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss.000
      .mode("append") // Optional, default: overwrite.
      .save(OUTPUT_FILE_PATH)
    spark.stop()
  }

}
