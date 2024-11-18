import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import ch.hsr.geohash.GeoHash

object WeatherJoin {
 // Geohash generation function - 4 chars precision
 def generateGeohash = udf((lat: Double, lng: Double) => {
   if (lat != null && lng != null) {
     try {
       GeoHash.withCharacterPrecision(lat, lng, 4).toBase32
     } catch {
       case _: Exception => null
     }
   } else {
     null
   }
 })

 def main(args: Array[String]): Unit = {
   val spark = SparkSession.builder()
     .appName("weather-join")
     .master("local[*]")
     .getOrCreate()

    //still don't understand why this doesn't work if imported on top
   import spark.implicits._

   try {
     // fixed restaurant data
     println("EPAM: fixed restaurant data:")
     val restaurantDF = spark.read
       .parquet("data/enriched/restaurants_with_coords")

     // Generate geohashes for restaurants
     println("EPAM: creating geohashes for restaurants:")
     val restaurantWithGeohashDF = restaurantDF
       .withColumn("geohash", generateGeohash(col("final_lat"), col("final_lng")))

     // processing weather dataset, only going to use one date, 15th of oct, to avoid multiplications
     println("EPAM: processing weather data:")
     val weatherDF = spark.read
       .parquet("data/weather/year=2016/month=10")
       .where(col("wthr_date") === "2016-10-15") 
       .withColumn("geohash", generateGeohash(col("lat"), col("lng")))      
       .groupBy("geohash")
       .agg(
         avg("avg_tmpr_c").as("avg_tmpr_c"),
         first("wthr_date").as("wthr_date")
       )

     // joining datasets
     println("EPAM: joining restaurant with weather:")
     val enrichedDF = restaurantWithGeohashDF
       .join(weatherDF, Seq("geohash"), "left")

     // join statistics for validation if it worked
     val joinStats = enrichedDF.agg(
       count("*").as("total_restaurants"),
       count("avg_tmpr_c").as("with_weather_data"),
       (count("avg_tmpr_c") * 100.0 / count("*")).as("match_percentage")
     ).collect()(0)

     println("\nEPAM: join statistics:")
     println(s"EPAM: total num of restaurants: ${joinStats.getLong(0)}")
     println(s"EPAM: with weather data: ${joinStats.getLong(1)}")
     println(s"matching rate: ${joinStats.getDouble(2)}%")

     // top 5 of joined data
     println("\n EPAM: top 5 of enriched data:")
     enrichedDF
       .select("id", "city", "country", "geohash", "avg_tmpr_c", "wthr_date")
       .show(5)

     // per task saving enriched data
     println("\nEPAM: saving enriched data into the folder:")
     enrichedDF.write
       .partitionBy("country", "city")
       .mode("overwrite")
       .parquet("data/enriched/final")

     println("EPAM: Woohoo!! completed successfully!")

   } catch {
     case e: Exception =>
       println(s"Error in join process: ${e.getMessage}")
       e.printStackTrace()
   } finally {
     spark.stop()
   }
 }
}