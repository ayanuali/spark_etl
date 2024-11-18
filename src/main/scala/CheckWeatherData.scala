import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CheckWeatherData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("check-weather-data")
      .master("local[*]")
      .getOrCreate()

    //same, not sure why it kept failing, if imported on top
    import spark.implicits._

    try {
      // weather data for only october 2016
      val weatherDF = spark.read
        .parquet("data/weather/year=2016/month=10")


      println("\nEPAM: weaather dataset schema for FYI:")
      weatherDF.printSchema()

      // some records for validation
      println("\nEPAM: top 5 records:")
      weatherDF.show(5)

      // validate data ranges
      println("\nEPAM: dataset summary:")
      weatherDF.describe("lat", "lng", "avg_tmpr_c").show()

      // counting total number of records
      val totalRecords = weatherDF.count()
      println(s"\nEPAM: total number of weather records: $totalRecords")

      // validate date range, just to make sure that it indeed covers from 1st to 31st of oct
      println("\nEPAM: weather date range:")
      weatherDF.select(
        min("wthr_date").as("start_date"),
        max("wthr_date").as("end_date")
      ).show()

    } finally {
      spark.stop()
    }
  }
}