package com.urbancomputing.dbscan

import scala.math.{pow, sqrt}

case class DBSCANPoint(x: Double, y: Double) extends Serializable {
  /**
   *
   * @param other other point
   * @return is equal or less than eps
   */
  def distance(other: DBSCANPoint): Double =
    sqrt(pow(x - other.x, 2) + pow(y - other.y, 2))
}

