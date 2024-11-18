import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.io.Source
import java.net.URLEncoder
import scala.util.{Try, Success, Failure}
import org.json4s._
import org.json4s.jackson.JsonMethods._

object RestaurantETL {
  def geocodeAddress(city: String, country: String, apiKey: String): Option[(Double, Double)] = {
    val query = URLEncoder.encode(s"$city, $country", "UTF-8")
    //url to query the opencage api
    val url = s"https://api.opencagedata.com/geocode/v1/json?q=$query&key=$apiKey&limit=1"
    
    Try {
      val response = Source.fromURL(url).mkString
      //using json4s lib
      implicit val formats = DefaultFormats
      val json = parse(response)
      
      val coordinates = for {
        results <- (json \ "results").extractOpt[List[JValue]]
        firstResult <- results.headOption
        geometry <- (firstResult \ "geometry").extractOpt[JValue]
        lat <- (geometry \ "lat").extractOpt[Double]
        lng <- (geometry \ "lng").extractOpt[Double]
      } yield (lat, lng)
      
      coordinates
    } match {
      case Success(Some((lat, lng))) => Some((lat, lng))
      case _ => None
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      //checking if key provided
      println("EPAM: Key value RestaurantETL <opencage_api_key>")
      System.exit(1)
    }

    val opencageApiKey = args(0)
    val spark = SparkSession.builder()
      .appName("restaurant-etl")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    try {
      // restaurant data
      val restaurantDF = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/restaurant_csv")

      // register geocoding UDF
      val geocodeUDF = spark.udf.register("geocode",
        (city: String, country: String) => geocodeAddress(city, country, opencageApiKey))

      // then applying geocoding
      val geocodedDF = restaurantDF
        .withColumn("needs_geocoding",
          col("lat").isNull || 
          col("lng").isNull || 
          col("lat") === 0.0 || 
          col("lng") === 0.0
        )
        .withColumn("geocoded_coords",
          when(col("needs_geocoding"), callUDF("geocode", col("city"), col("country")))
        )
        .withColumn("final_lat",
          coalesce(
            col("geocoded_coords._1"),
            when(col("needs_geocoding"), null).otherwise(col("lat"))
          )
        )
        .withColumn("final_lng",
          coalesce(
            col("geocoded_coords._2"),
            when(col("needs_geocoding"), null).otherwise(col("lng"))
          )
        )
        .drop("geocoded_coords", "needs_geocoding")

      // just in case going to save fixed values
      geocodedDF.write
        .mode("overwrite")
        .parquet("data/enriched/restaurants_with_coords")

      // for validation showing fixed values
      println("\nEPAM: fixed records:")
      geocodedDF
      .filter(
        col("lat").isNull || 
        col("lng").isNull ||
        col("lat") =!= col("final_lat") || 
        col("lng") =!= col("final_lng")
      )
      .select("id", "city", "country", "lat", "lng", "final_lat", "final_lng")
      .show()

    } finally {
      spark.stop()
    }
  }
}