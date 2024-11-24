package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    val rect_pos = queryRectangle.split(',')
    val rect_x_min = rect_pos(0).trim.toDouble
    val rect_y_min = rect_pos(1).trim.toDouble
    val rect_x_max = rect_pos(2).trim.toDouble
    val rect_y_max = rect_pos(3).trim.toDouble

    val pt_pos = pointString.split(',')
    val pt_x = pt_pos(0).trim.toDouble
    val pt_y = pt_pos(1).trim.toDouble

    return (rect_x_min <= pt_x && pt_x <= rect_x_max) && (
      rect_y_min <= pt_y && pt_y <= rect_y_max
      )
  }

}
