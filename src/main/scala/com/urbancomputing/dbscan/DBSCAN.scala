package com.urbancomputing.dbscan

import com.urbancomputing.dbscan.DBSCANLabeledPoint.Flag
import org.apache.spark.rdd.RDD

object DBSCAN {
  /**
   * Train a DBSCAN Model using the given set of parameters
   */
  def train(data: RDD[DBSCANPoint],
            eps: Double,
            minPoints: Int,
            maxPointsPerPartition: Int): DBSCAN =
    new DBSCAN(eps, minPoints, maxPointsPerPartition, null, null).train(data)
}

/**
 * A parallel implementation of DBSCAN clustering. The implementation will split the data space
 * into a number of partitions, making a best effort to keep the number of points in each
 * partition under `maxPointsPerPartition`. After partitioning, traditional DBSCAN
 * clustering will be run in parallel for each partition and finally the results
 * of each partition will be merged to identify global clusters.
 *
 * This is an iterative algorithm that will make multiple passes over the data,
 * any given RDDs should be cached by the user.
 */
class DBSCAN private(val eps: Double,
                     val minPoints: Int,
                     val maxPointsPerPartition: Int,
                     @transient val partitions: List[(Int, DBSCANRectangle)],
                     @transient private val labeledPartitionedPoints: RDD[(Int, DBSCANLabeledPoint)]) extends Serializable {
  type Margins = (DBSCANRectangle, DBSCANRectangle, DBSCANRectangle)
  type ClusterId = (Int, Int)

  private def minimumRectangleSize: Double = 2 * eps

  def labeledPoints: RDD[DBSCANLabeledPoint] = labeledPartitionedPoints.values

  private def train(data: RDD[DBSCANPoint]): DBSCAN = {
    // generate the smallest rectangles that split the space
    // and count how many points are contained in each one of them
    val minimumRectanglesWithCount =
    data
      .map(toMinimumBoundingRectangle)
      .map((_, 1))
      .aggregateByKey(0)(_ + _, _ + _)
      .collect()
      .toSet

    println("基础分区")
    minimumRectanglesWithCount.foreach(r => println(r._1.toString))

    // find the best partitions for the data space
    val localPartitions = EvenSplitPartitioner
      .partition(minimumRectanglesWithCount, maxPointsPerPartition, minimumRectangleSize)

    // print partition info
    println(s"分区内最大点数：$maxPointsPerPartition，分区个数: ${localPartitions.size}")
    localPartitions.foreach(p => println(p._1.toString))

    // grow partitions to include eps
    val localMargins =
      localPartitions
        .map({ case (p, _) => (p.shrink(eps), p, p.shrink(-eps)) })
        .zipWithIndex

    println("缩小后的分区：")
    localMargins.foreach(r => println(r._1._1.toString))

    println("扩大后的分区：")
    localMargins.foreach(r => println(r._1._3.toString))

    // assign each point to its proper partition
    val margins = data.context.broadcast(localMargins)
    val duplicated = for {
      point <- data
      ((_, _, outer), id) <- margins.value
      if outer.contains(point)
    } yield (id, point)

    // perform local dbscan
    val numOfPartitions = localPartitions.size
    val clustered =
      duplicated
        .groupByKey(numOfPartitions)
        .flatMapValues(points => new LocalDBSCANArchery(eps, minPoints).fit(points))
        .cache()

    // find all candidate points for merging clusters and group them
    val mergePoints =
      clustered
        .flatMap({
          case (partition, point) =>
            margins.value
              .filter({
                case ((inner, main, _), _) => main.contains(point) && !inner.almostContains(point)
              })
              .map({
                case (_, newPartition) => (newPartition, (partition, point))
              })
        })
        .groupByKey()

    // find all clusters with aliases from merging candidates
    val adjacencies =
      mergePoints
        .flatMapValues(findAdjacencies)
        .values
        .collect()

    // generated adjacency graph
    val adjacencyGraph = adjacencies.foldLeft(DBSCANGraph[ClusterId]()) {
      case (graph, (from, to)) => graph.connect(from, to)
    }

    // find all cluster ids
    val localClusterIds =
      clustered
        .filter({ case (_, point) => point.flag != Flag.Noise })
        .mapValues(_.cluster)
        .distinct()
        .collect()
        .toList

    // assign a global Id to all clusters, where connected clusters get the same id
    val (_, clusterIdToGlobalId) = localClusterIds.foldLeft((0, Map[ClusterId, Int]())) {
      case ((id, map), clusterId) =>
        map.get(clusterId) match {
          case None =>
            val nextId = id + 1
            val connectedClusters = adjacencyGraph.getConnected(clusterId) + clusterId
            val toAdd = connectedClusters.map((_, nextId)).toMap
            (nextId, map ++ toAdd)
          case Some(_) => (id, map)
        }
    }

    // relabel non-duplicated points
    val clusterIds = data.context.broadcast(clusterIdToGlobalId)
    val labeledInner =
      clustered
        .filter(isInnerPoint(_, margins.value))
        .map {
          case (partition, point) =>
            if (point.flag != Flag.Noise) {
              point.cluster = clusterIds.value((partition, point.cluster))
            }
            (partition, point)
        }

    // de-duplicate and label merge points
    val labeledOuter =
      mergePoints.flatMapValues(partition => {
        partition.foldLeft(Map[DBSCANPoint, DBSCANLabeledPoint]())({
          case (all, (partition, point)) =>
            if (point.flag != Flag.Noise) {
              point.cluster = clusterIds.value((partition, point.cluster))
            }
            all.get(point) match {
              case None => all + (point -> point)
              case Some(prev) =>
                // override previous entry unless new entry is noise
                if (point.flag != Flag.Noise) {
                  prev.flag = point.flag
                  prev.cluster = point.cluster
                }
                all
            }
        }).values
      })

    val finalPartitions = localMargins.map {
      case ((_, p, _), index) => (index, p)
    }

    new DBSCAN(
      eps,
      minPoints,
      maxPointsPerPartition,
      finalPartitions,
      labeledInner.union(labeledOuter))
  }

  private def isInnerPoint(entry: (Int, DBSCANLabeledPoint),
                           margins: List[(Margins, Int)]): Boolean =
    entry match {
      case (partition, point) =>
        val ((inner, _, _), _) = margins.filter({
          case (_, id) => id == partition
        }).head
        inner.almostContains(point)
    }

  private def findAdjacencies(partition: Iterable[(Int, DBSCANLabeledPoint)]): Set[((Int, Int), (Int, Int))] = {
    val zero = (Map[DBSCANPoint, ClusterId](), Set[(ClusterId, ClusterId)]())
    val (_, adjacencies) = partition.foldLeft(zero)({
      case ((seen, adjacencies), (partition, point)) =>
        // noise points are not relevant for adjacencies
        if (point.flag == Flag.Noise) {
          (seen, adjacencies)
        } else {
          val clusterId = (partition, point.cluster)
          seen.get(point) match {
            case None => (seen + (point -> clusterId), adjacencies)
            case Some(prevClusterId) => (seen, adjacencies + ((prevClusterId, clusterId)))
          }
        }
    })
    adjacencies
  }

  private def toMinimumBoundingRectangle(dbscanPoint: DBSCANPoint): DBSCANRectangle = {
    val x = corner(dbscanPoint.x)
    val y = corner(dbscanPoint.y)
    DBSCANRectangle(x, y, x + minimumRectangleSize, y + minimumRectangleSize)
  }

  private def corner(p: Double): Double =
    (shiftIfNegative(p) / minimumRectangleSize).intValue * minimumRectangleSize

  private def shiftIfNegative(p: Double): Double =
    if (p < 0) p - minimumRectangleSize else p
}

