import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll

class WeatherJoinTests extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  
  val spark = SparkSession.builder()
    .appName("weather-join-test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  "WeatherJoin" should "generate valid geohashes" in {
    val testDF = Seq(
      (51.5074, -0.1278), // London for testing
      (48.8566, 2.3522)   // Paris for testing
    ).toDF("lat", "lng")

    val withGeohashDF = testDF
      .withColumn("geohash", WeatherJoin.generateGeohash(col("lat"), col("lng")))

    withGeohashDF.filter(col("geohash").isNull).count() shouldBe 0

    val geohashes = withGeohashDF.select("geohash").collect().map(_.getString(0))
    geohashes.foreach { hash =>
      hash.length shouldBe 4
    }
  }

  it should "join restaurant and weather dataset" in {
    val restaurantDF = Seq(
      (1L, "London", "GB", 51.5074, -0.1278),
      (2L, "Paris", "FR", 48.8566, 2.3522)
    ).toDF("id", "city", "country", "final_lat", "final_lng")

    //taking only 15th of october, to avoid duplicates
    val weatherDF = Seq(
      (51.5074, -0.1278, 15.5, "2016-10-15"),
      (48.8566, 2.3522, 18.2, "2016-10-15")
    ).toDF("lat", "lng", "avg_tmpr_c", "wthr_date")

    val restaurantWithGeohashDF = restaurantDF
      .withColumn("geohash", WeatherJoin.generateGeohash(col("final_lat"), col("final_lng")))

    val weatherWithGeohashDF = weatherDF
      .withColumn("geohash", WeatherJoin.generateGeohash(col("lat"), col("lng")))
      .groupBy("geohash")
      .agg(
        avg("avg_tmpr_c").as("avg_tmpr_c"),
        first("wthr_date").as("wthr_date")
      )

    val joinedDF = restaurantWithGeohashDF
      .join(weatherWithGeohashDF, Seq("geohash"), "left")

    joinedDF.count() shouldBe 2
    joinedDF.filter(col("avg_tmpr_c").isNotNull).count() shouldBe 2
    
    val results = joinedDF.select("city", "avg_tmpr_c").collect()
    results.find(_.getString(0) == "London").get.getDouble(1) shouldBe 15.5
    results.find(_.getString(0) == "Paris").get.getDouble(1) shouldBe 18.2
  }

  it should "take care of edge cases in joining" in {
    val restaurantDF = Seq(
      (1L, "Invalid", "XX", Option.empty[Double], Option.empty[Double]),  // Null coordinates
      (2L, "Zero", "YY", Some(0.0), Some(0.0))        // Zero coordinates
    ).toDF("id", "city", "country", "final_lat", "final_lng")

    val weatherDF = Seq(
      (51.5074, -0.1278, 15.5, "2016-10-15")
    ).toDF("lat", "lng", "avg_tmpr_c", "wthr_date")

    val restaurantWithGeohashDF = restaurantDF
      .withColumn("geohash", WeatherJoin.generateGeohash(col("final_lat"), col("final_lng")))

    val weatherWithGeohashDF = weatherDF
      .withColumn("geohash", WeatherJoin.generateGeohash(col("lat"), col("lng")))
      .groupBy("geohash")
      .agg(
        avg("avg_tmpr_c").as("avg_tmpr_c"),
        first("wthr_date").as("wthr_date")
      )

    val joinedDF = restaurantWithGeohashDF
      .join(weatherWithGeohashDF, Seq("geohash"), "left")

    joinedDF.count() shouldBe 2
    joinedDF.filter(col("avg_tmpr_c").isNotNull).count() shouldBe 0
  }

  override def afterAll(): Unit = {
    spark.stop()
  }
}