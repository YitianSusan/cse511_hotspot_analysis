package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession,SaveMode}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  val logger: Logger = Logger.getLogger(this.getClass.getName)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ", (pickupTime: String) => ((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
    pickupInfo.createOrReplaceTempView("pickupInfo")
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

    // Step 1: Count trips per cell
    val tripsPerCell = spark.sql(
      s"""
         |SELECT
         |  x,
         |  y,
         |  z,
         |  COUNT(*) as trips_cnt
         |FROM pickupInfo
         |WHERE
         |x >= $minX AND x <= $maxX
         |AND y >= $minY AND y <= $maxY
         |AND z >= $minZ AND z <= $maxZ
         |GROUP BY x, y, z
         |""".stripMargin
    )
    logger.info("tripsPerCell")
    tripsPerCell.show()
    tripsPerCell.createOrReplaceTempView("tripsPerCell")
//    tripsPerCell.write.mode(SaveMode.Overwrite).csv("/Users/yitiansusanlin/Documents/ASU/CSE511 data processing/project 2/cse511_hotspot_analysis/CSE511-Project-Hotspot-Analysis/test/tripsPerCell.csv")

    // Step 2: Compute global statistics
    val globalStats = spark.sql(
      """
        |SELECT
        | SUM(trips_cnt) as sum_x,
        | SUM(trips_cnt * trips_cnt) as sum_sq_x
        | FROM tripsPerCell
        |""".stripMargin
    ).collect()(0)

    val sum_x = globalStats.getLong(0)
    val sum_sq_x = globalStats.getLong(1)
    val mean = sum_x / numCells
    val std = Math.sqrt((sum_sq_x / numCells) - Math.pow(mean, 2))
    logger.info("mean = " + mean + "; std = " + std)

    // Step 3: calculate neighbor info
    // cartesian product of tripsPerCell table with itself to calculate neighbors info
    // neighbor: x, y, z is +- 1 from itself but not fully equal
    val neighborData = spark.sql(
      s"""
         |SELECT
         |  tp1.x,
         |  tp1.y,
         |  tp1.z,
         |  SUM(tp2.trips_cnt) AS sum_neighbour_trips,
         |  COUNT(tp1.x, tp1.y, tp1.z) AS num_neighbors
         |FROM tripsPerCell tp1, tripsPerCell tp2
         |WHERE
         |  ABS(tp1.x - tp2.x) <= 1 AND
         |  ABS(tp1.y - tp2.y) <= 1 AND
         |  ABS(tp1.z - tp2.z) <= 1
         |GROUP BY tp1.x, tp1.y, tp1.z
         |ORDER BY sum_neighbour_trips DESC
         |""".stripMargin
    )
    logger.info("neighborData")
    neighborData.show()
    neighborData.createOrReplaceTempView("neighborData")
//    neighborData.write.mode(SaveMode.Overwrite).csv("/Users/yitiansusanlin/Documents/ASU/CSE511 data processing/project 2/cse511_hotspot_analysis/CSE511-Project-Hotspot-Analysis/test/neighborData.csv")

    // Step 3: Calculate G* statistic for each cell
    // Register calculateG as a UDF
    spark.udf.register("calculateG", (sumNeighborTrips, cntNeighbor) =>
      HotcellUtils.calculateG(sumNeighborTrips, cntNeighbor, numCells, mean, std)
    )

    val gStarResults = spark.sql(
      s"""
         |SELECT
         |  x,
         |  y,
         |  z,
         |  calculateG(sum_neighbour_trips, num_neighbors) as gScore
         |FROM neighborData
         |ORDER BY gScore DESC
         |LIMIT 50
         |""".stripMargin
    )
    gStarResults.createOrReplaceTempView("gStarResults")
    gStarResults.show()

    return gStarResults.select("x", "y", "z")
  }
}
