import com.mongodb.spark.MongoSpark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}

object SampleApp {

  final val OUTPUT_FILE_PATH = "output.xlsx"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = getSparkSession
    process(spark)
    spark.stop()
  }

  private def process(spark: SparkSession): Unit = {
    val df = readInput(spark).toDF()
    df.registerTempTable("call_details")
    df.show(3)
    val data = spark.sql("SELECT SR_TYPE FROM call_details")
    data.show
    val filteredDuplicateDF = df.filter("DUPLICATE = true").groupBy(df("SR_TYPE")).count //.select(df("CITY"))
    val filteredStatusDF = df.groupBy(df("STATUS")).count
    val filteredCityDF = df.groupBy(df("CITY")).count
    insertIntoMongo(filteredDuplicateDF, filteredStatusDF, filteredCityDF)
    insertIntoExccel(filteredDuplicateDF, filteredStatusDF, filteredCityDF)
  }

  private def insertIntoMongo(filteredDuplicateDF: DataFrame, filteredStatusDF: DataFrame, filteredCityDF: DataFrame): Unit = {
    writeToMongo(filteredDuplicateDF, "DUPLICATE_COUNT")
    writeToMongo(filteredStatusDF, "STATUS_COUNT")
    writeToMongo(filteredCityDF, "CITY_COUNT")
  }

  private def readInput(spark: SparkSession) = {
    spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      //.load("file:/home/ubuntu/311data.csv")
      .load("hdfs://localhost:9000/demo/311data.csv")
  }

  private def getSparkSession = {
    SparkSession.builder
      .appName("SampleApp")
      .master("local[4]")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/spark.myCollection")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/spark.myCollection")
      .getOrCreate()
  }

  private def insertIntoExccel(filteredDuplicateDF: DataFrame, filteredStatusDF: DataFrame, filteredCityDF: DataFrame): Unit = {
    writeTpExcel(filteredDuplicateDF, "Duplicate_Count[#All]")
    writeTpExcel(filteredStatusDF, "Status_Count[#All]")
    writeTpExcel(filteredCityDF, "City_Count[#All]")
  }

  private def writeToMongo(df: DataFrame, collection: String): Unit = {
    MongoSpark.save(df.write.option("collection", collection).mode("overwrite").format("com.mongodb.spark.sql"))
  }

  private def writeTpExcel(df: DataFrame, sheetName: String): Unit = {
    df.write
      .format("com.crealytics.spark.excel")
      .option("dataAddress", sheetName)
      .option("useHeader", "true")
      .option("dateFormat", "yy-mmm-d") // Optional, default: yy-m-d h:mm
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss.000
      .mode("append") // Optional, default: overwrite.
      .save(OUTPUT_FILE_PATH)
  }
}
