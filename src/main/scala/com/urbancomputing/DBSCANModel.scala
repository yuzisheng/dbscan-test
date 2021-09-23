package com.urbancomputing

import com.urbancomputing.dbscan.{DBSCAN, DBSCANPoint, LocalDBSCANArchery, LocalDBSCANNaive}
import org.apache.spark.rdd.RDD

object DBSCANModel {

  /**
   * 分布式模型
   */
  def distributedDBScan(data: RDD[DBSCANPoint], eps: Double, minPoints: Int, maxPointsPerPartition: Int): Long = {
    val distributedModel = DBSCAN.train(data, eps, minPoints, maxPointsPerPartition)
    // distributedModel.labeledPoints.collect().foreach(p => println(s"${p.x},${p.y},${p.cluster}"))
    // val labeledPointNumber = distributedModel.labeledPoints.count()  // 聚类总点数
    val clusterNumber = distributedModel.labeledPoints.map(_.cluster).distinct().count() // 聚类个数
    clusterNumber
  }

  /**
   * 单机朴素模型
   */
  def localNativeDBScan(data: RDD[DBSCANPoint], eps: Double, minPoints: Int): Long = {
    val localNativeModel = new LocalDBSCANNaive(eps, minPoints)
    val labeledPoints = localNativeModel.fit(data.collect())
    // labeledPoints.foreach(p => println(s"${p.x},${p.y},${p.cluster}"))
    val clusterNumber = labeledPoints.map(_.cluster).toSet.size
    clusterNumber
  }

  /**
   * 单机RTree模型
   */
  def localRTreeDBScan(data: RDD[DBSCANPoint], eps: Double, minPoints: Int): Long = {
    val localRTreeModel = new LocalDBSCANArchery(eps, minPoints)
    val labeledPoints = localRTreeModel.fit(data.collect())
    // labeledPoints.foreach(p => println(s"${p.x},${p.y},${p.cluster}"))
    val clusterNumber = labeledPoints.map(_.cluster).toSet.size
    clusterNumber
  }
}
