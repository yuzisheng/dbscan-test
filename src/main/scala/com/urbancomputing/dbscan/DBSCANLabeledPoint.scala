package com.urbancomputing.dbscan

/**
 * Companion constants for labeled points
 */
object DBSCANLabeledPoint {
  val Unknown = 0

  object Flag extends Enumeration {
    type Flag = Value
    val Border, Core, Noise, NotFlagged = Value
  }
}

class DBSCANLabeledPoint(x: Double, y: Double) extends DBSCANPoint(x, y) {
  def this(p: DBSCANPoint) = this(p.x, p.y)

  var flag: DBSCANLabeledPoint.Flag.Value = DBSCANLabeledPoint.Flag.NotFlagged
  var cluster: Int = DBSCANLabeledPoint.Unknown
  var visited = false

  override def toString: String = s"x=$x,y=$y,cluster=$cluster,flag=$flag"
}

