import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CheckEnrichedData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("check-enriched")
      .master("local[*]")
      .getOrCreate()

    try {
      val enrichedDF = spark.read.parquet("data/enriched/final")

      // Print schema
      println("\nEnriched Data Schema:")
      enrichedDF.printSchema()

      // Show sample data
      println("\nSample of Enriched Records:")
      enrichedDF.select(
        "id", "franchise_name", "country", "city",
        "final_lat", "final_lng", "geohash", 
        "avg_tmpr_c", "wthr_date"
      ).show(5)

      // Check data completeness
      val stats = enrichedDF.agg(
        count("*").as("total_records"),
        sum(when(col("avg_tmpr_c").isNotNull, 1).otherwise(0)).as("records_with_weather"),
        countDistinct("geohash").as("unique_geohashes"),
        countDistinct("wthr_date").as("unique_dates")
      ).collect()(0)

      println("\nData Statistics:")
      println(s"Total records: ${stats.getLong(0)}")
      println(s"Records with weather data: ${stats.getLong(1)}")
      println(s"Unique geohashes: ${stats.getLong(2)}")
      println(s"Unique dates: ${stats.getLong(3)}")

      // Check partitioning
      println("\nPartition Distribution:")
      enrichedDF.groupBy("country")
        .count()
        .orderBy(desc("count"))
        .show(5)

    } finally {
      spark.stop()
    }
  }
}