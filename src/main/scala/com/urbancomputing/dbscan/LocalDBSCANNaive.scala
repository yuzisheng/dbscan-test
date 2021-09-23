package com.urbancomputing.dbscan

import com.urbancomputing.dbscan.DBSCANLabeledPoint.Flag

import scala.collection.mutable


/**
 * A naive implementation of DBSCAN. It has O(n2) complexity
 * but uses no extra memory. This implementation is not used
 * by the parallel version of DBSCAN.
 *
 */
class LocalDBSCANNaive(eps: Double, minPoints: Int) {
  val minDistanceSquared: Double = eps * eps

  def fit(points: Iterable[DBSCANPoint]): Iterable[DBSCANLabeledPoint] = {
    val labeledPoints = points.map(new DBSCANLabeledPoint(_)).toArray
    labeledPoints
      .foldLeft(DBSCANLabeledPoint.Unknown)(
        (cluster, point) => {
          if (!point.visited) {
            point.visited = true
            val neighbors = findNeighbors(point, labeledPoints)
            if (neighbors.size < minPoints) {
              point.flag = Flag.Noise
              cluster
            } else {
              expandCluster(point, neighbors, labeledPoints, cluster + 1)
              cluster + 1
            }
          } else {
            cluster
          }
        })
    labeledPoints
  }

  private def findNeighbors(point: DBSCANPoint,
                            all: Array[DBSCANLabeledPoint]): Iterable[DBSCANLabeledPoint] =
    all.view.filter(other => point.distance(other) <= eps)

  def expandCluster(point: DBSCANLabeledPoint,
                    neighbors: Iterable[DBSCANLabeledPoint],
                    all: Array[DBSCANLabeledPoint],
                    cluster: Int): Unit = {
    point.flag = Flag.Core
    point.cluster = cluster

    val allNeighbors = mutable.Queue(neighbors)

    while (allNeighbors.nonEmpty) {
      allNeighbors.dequeue().foreach(neighbor => {
        if (!neighbor.visited) {
          neighbor.visited = true
          neighbor.cluster = cluster
          val neighborNeighbors = findNeighbors(neighbor, all)
          if (neighborNeighbors.size >= minPoints) {
            neighbor.flag = Flag.Core
            allNeighbors.enqueue(neighborNeighbors)
          } else {
            neighbor.flag = Flag.Border
          }
          if (neighbor.cluster == DBSCANLabeledPoint.Unknown) {
            neighbor.cluster = cluster
            neighbor.flag = Flag.Border
          }
        }
      })
    }
  }
}
