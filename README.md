# Restaurant Weather Data Enrichment

## Environment Setup on my Mac
- Java 17.0.13 (temurin) (I couldn't use java 21, too many conflicts with scala and spark)
- Scala 2.12.18 
- Spark 3.5.3
- Sublime Text for coding

## Project Structure
```
epam-spark-etl-task/
├── build.sbt
├── src/main/scala/
│   ├── CheckData.scala             # Restaurant data validation
│   ├── CheckWeatherData.scala      # Weather data validation
│   ├── RestaurantETL.scala         # Task: Coordinates fixing
│   ├── WeatherJoin.scala           # Task: Join and enrichment
│   └── CheckJoin.scala             # To make sure we verify result
└── data/
    ├── restaurant_csv/             # restaurant dataset
    ├── weather/year=2016/month=10/ # weather dataset
    └── enriched/                   # dir for enriched data
        ├── restaurants_with_coords/ # for coordinates fix results
        └── final/                  # final vesrion of the data
```

## Implementation Steps

### 1. Data Analysis
First, checked data quality of both datasets:
```scala
// Check restaurant data
sbt "runMain CheckData"
```
Results:
- Total restaurants: 1997
- 2 records with invalid coordinates:
  - London (lat: 51.502, lng: 0.0)
  - Dillon (NULL coordinates)

```scala
// Check weather data, I only took October 2016, whole month
sbt "runMain CheckWeatherData"
```
Weather data structure:
- Date range: October 2016
- Contains: temperature, coordinates
- Valid coordinate ranges

### 2. Fix Restaurant Coordinates
Created RestaurantETL to fix invalid coordinates using OpenCage
```scala
sbt "runMain RestaurantETL OPENCAGE_API_KEY"
```
- Used OpenCage api for validation
- Handles nulls and zeros in coordinates
- Saves fixed data to dir as parquet files

### 3. Weather Data Join
Made join using geohashes:
```scala
sbt "runMain WeatherJoin"
```
What was done:
- Generated 4-char geohashes for both datasets
- Used left join to keep all restaurants
- Avoided data multiplication by: (at first had issue of data getting multiplied)
  - Taking one day's weather (Oct 15)
  - Averaging temperatures per geohash
- Results partitioned by country and city

### 4. Results Verification
Checked final enriched data:
```scala
sbt "runMain CheckJoin"
```
Results:
- Total restaurants: 1997 (the number is the same as restaurants data validation)
- Weather data match rate was 94%
- properly partitioned data
- Fixed coordinates

## Building and Running
1. Clone repo from github:
```bash
git clone <https://github.com/ayanuali/spark_etl>
cd epam-spark-etl-task
```

2. Created build.sbt, the code is in the repo
After, run 
```bash
#clean and download dependencies
sbt clean compile
```

3. Run code for data validation:
```bash
# Check restaurant data first
sbt "runMain CheckData"

#top 5 from restaurant dataset
# [info] +------------+------------+----------------+-----------------------+-------+----------+------+-------+
# [info] |          id|franchise_id|  franchise_name|restaurant_franchise_id|country|      city|   lat|    lng|
# [info] +------------+------------+----------------+-----------------------+-------+----------+------+-------+
# [info] |197568495625|          10|The Golden Spoon|                  24784|     US|   Decatur|34.578|-87.021|
# [info] | 17179869242|          59|     Azalea Cafe|                  10902|     FR|     Paris|48.861|  2.368|
# [info] |214748364826|          27| The Corner Cafe|                  92040|     US|Rapid City| 44.08|-103.25|
# [info] |154618822706|          51|    The Pizzeria|                  41484|     AT|    Vienna|48.213| 16.413|
# [info] |163208757312|          65|   Chef's Corner|                  96638|     GB|    London|51.495| -0.191|
# [info] +------------+------------+----------------+-----------------------+-------+----------+------+-------+
# [info] only showing top 5 rows

#invalid coordinates
# [info] +------------+------------+---------------+-----------------------+-------+------+------+----+
# [info] |          id|franchise_id| franchise_name|restaurant_franchise_id|country|  city|   lat| lng|
# [info] +------------+------------+---------------+-----------------------+-------+------+------+----+
# [info] |171798691894|          55|The Steak House|                  65939|     GB|London|51.502| 0.0|
# [info] | 85899345920|           1|        Savoria|                  18952|     US|Dillon|  NULL|NULL|
# [info] +------------+------------+---------------+-----------------------+-------+------+------+----+


#then check weather data
sbt "runMain CheckWeatherData"

# [info] EPAM: weaather dataset schema for FYI:
# [info] root
# [info]  |-- lng: double (nullable = true)
# [info]  |-- lat: double (nullable = true)
# [info]  |-- avg_tmpr_f: double (nullable = true)
# [info]  |-- avg_tmpr_c: double (nullable = true)
# [info]  |-- wthr_date: string (nullable = true)
# [info]  |-- day: integer (nullable = true)

#top 5 records
# [info] +--------+-------+----------+----------+----------+---+
# [info] |     lng|    lat|avg_tmpr_f|avg_tmpr_c| wthr_date|day|
# [info] +--------+-------+----------+----------+----------+---+
# [info] |-111.202|18.7496|      82.7|      28.2|2016-10-12| 12|
# [info] |-111.155| 18.755|      82.7|      28.2|2016-10-12| 12|
# [info] |-111.107|18.7604|      82.7|      28.2|2016-10-12| 12|
# [info] |-111.059|18.7657|      82.5|      28.1|2016-10-12| 12|
# [info] |-111.012|18.7711|      82.5|      28.1|2016-10-12| 12|
# [info] +--------+-------+----------+----------+----------+---+

# Fix coordinates
export OPENCAGE_API_KEY=your_key_here
sbt "runMain RestaurantETL $OPENCAGE_API_KEY"

# Join with weather
sbt "runMain WeatherJoin"

# Verify results just in case
sbt "runMain CheckJoin"
```

### Tests
Run RestaurantETLTests test:
```bash
sbt "testOnly *RestaurantETLTests"
```

## Notes and comments
- All development done locally on Mac, no Docker used
- Used Sublime Text for code editing
- OpenCage api key aquired from the official website
- fixed coordinates results stored for debugging purpose
- Finally, enriched data partitioned by country/city

## More comments
- OpenCage api has limited capabilites, wasn't aware of that and thought something was wrong with my code
- Maybe I should have added more tests, but thought checking the main code and the join was enough