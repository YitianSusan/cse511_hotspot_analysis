package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int = {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(", "").toDouble / coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")", "").toDouble / coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser(timestampString: String): Timestamp = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def numNeighbors(x: Double, y: Double, z: Double, minX: Double,  maxX: Double, minY: Double, maxY: Double, minZ: Double, maxZ: Double) : Int= {
    var onAxis = 0
    if (x == minX || x == maxX) {
      onAxis += 1
    }
    if (y == minY || y == maxY) {
      onAxis += 1
    }
    if (z == minY || z == maxZ ) {
      onAxis += 1
    }
    if (onAxis == 0) {
      return 27
    } else if (onAxis== 1) {
      return 18
    } else if (onAxis == 2) {
      return 12
    } else if (onAxis == 3) {
      return 8
    }
    return 0
  }


  def calculateG(sum_neighbor_trips: Double, num_neighbors: Double, numCells: Double, mean: Double, std: Double): Double = {
    // cell: one unique (x, y, z) combination
    // x: # of rides picked up at a cell
    // n: total number of cells
    // weight: 1
    // j..n is all neighbors of cell i
    // formula reference: https://sigspatial2016.sigspatial.org/giscup2016/problem

    val numerator = sum_neighbor_trips - mean * num_neighbors
    val denominator = (std * math.sqrt(
      (numCells * num_neighbors  - Math.pow(num_neighbors, 2)) /
        (numCells - 1.0)))
    return numerator / denominator
  }
}
