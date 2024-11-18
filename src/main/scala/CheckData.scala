import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CheckData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("check-data")
      .master("local[*]")
      .getOrCreate()

    //had to move here, it kept failing for some reason
    import spark.implicits._

    try {
      // restaurant data
      val restaurantDF = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/restaurant_csv")

      //print out schema
      println("\nEPAM: Data Schema:")
      restaurantDF.printSchema()


      println("\nEPAM: Some records for validation:")
      restaurantDF.show(5)

      // invalid coordinates
      val invalidCoords = restaurantDF.filter(
        col("lat").isNull || 
        col("lng").isNull || 
        col("lat") === 0.0 || 
        col("lng") === 0.0
      )

      //printout invalid coords
      println("\nEPAM: invalid coordinates:")
      invalidCoords.show()

    } finally {
      spark.stop()
    }
  }
}