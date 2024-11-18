// src/test/scala/RestaurantETLTests.scala
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.functions._

class RestaurantETLTests extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  
  val spark = SparkSession.builder()
    .appName("restaurant-etl-test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // api key
  val apiKey = sys.env.getOrElse("OPENCAGE_API_KEY", 
    throw new Exception("didn't get OPENCAGE_API_KEY"))

  "RestaurantETL" should "geocode address successfully" in {
    val result = RestaurantETL.geocodeAddress("London", "GB", apiKey)
    result shouldBe defined
    result.foreach { case (lat, lng) =>
      lat should be >= 51.0
      lat should be <= 52.0
      lng should be >= -1.0
      lng should be <= 0.0
    }
  }

  it should "take care of invalid addresses" in {
    val result = RestaurantETL.geocodeAddress("NonExistentCity", "XX", apiKey)
    result shouldBe None
  }

  it should "process coordinates" in {
    val testDF = Seq(
      (1L, "London", "GB", 51.502, 0.0),
      (2L, "Paris", "FR", 48.8566, 2.3522)
    ).toDF("id", "city", "country", "lat", "lng")

    //UDF
    val geocodeUDF = spark.udf.register("geocode",
      (city: String, country: String) => RestaurantETL.geocodeAddress(city, country, apiKey))

    //code from main class
    val resultDF = testDF
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

    // for verification
    resultDF.filter(col("final_lng") =!= col("lng")).count() shouldBe 1
    resultDF.filter(col("final_lat").isNotNull).count() shouldBe 2
  }

  override def afterAll(): Unit = {
    spark.stop()
  }
}