package com.urbancomputing

import com.urbancomputing.dbscan.DBSCANPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  var sc: SparkContext = _

  /**
   * 加载数据
   */
  private def load(dataPath: String): RDD[DBSCANPoint] = {
    sc.textFile(dataPath).map(line => {
      val record = line.split(",")
      DBSCANPoint(record(0).toDouble, record(1).toDouble)
    })
  }

  /**
   * 不同半径EPS
   */
  def diffEps(epsList: Seq[Double], minPoints: Int, maxPointsPerPartition: Int,
              dataSize: Int, rootPath: String, method: String): Unit = {
    for (eps <- epsList) {
      val data = load(s"$rootPath/$dataSize.txt")
      val tic = System.currentTimeMillis()
      val clusterNumber = method match {
        case "local-native" => DBSCANModel.localNativeDBScan(data, eps, minPoints)
        case "local-rtree" => DBSCANModel.localRTreeDBScan(data, eps, minPoints)
        case "mr" => DBSCANModel.distributedDBScan(data, eps, minPoints, maxPointsPerPartition)
      }
      val tok = System.currentTimeMillis()
      val runtime = ((tok - tic) / 1000.0).formatted("%.2f")
      println(
        s"""++++++++++
           |[不同EPS测试: minPoints=$minPoints, maxPointPerPartition=$maxPointsPerPartition, dataSize=$dataSize, rootPath=$rootPath, method=$method]
           |EPS: $eps, 运行时间: $runtime 秒, 聚类个数: $clusterNumber
           |++++++++++""".stripMargin)
    }
  }

  /**
   * 不同最小点数MinPoints
   */
  def diffMinPoints(eps: Double, minPointsList: Seq[Int], maxPointsPerPartition: Int,
                    dataSize: Int, rootPath: String, method: String): Unit = {
    for (minPoints <- minPointsList) {
      val data = load(s"$rootPath/50000.txt")
      val tic = System.currentTimeMillis()
      val clusterNumber = method match {
        case "local-native" => DBSCANModel.localNativeDBScan(data, eps, minPoints)
        case "local-rtree" => DBSCANModel.localRTreeDBScan(data, eps, minPoints)
        case "mr" => DBSCANModel.distributedDBScan(data, eps, minPoints, maxPointsPerPartition)
      }
      val tok = System.currentTimeMillis()
      val runtime = ((tok - tic) / 1000.0).formatted("%.2f")
      println(
        s"""++++++++++
           |[不同MinPoints测试: eps=$eps, maxPointPerPartition=$maxPointsPerPartition, dataSize=$dataSize, rootPath=$rootPath, method=$method]
           |MinPoints: $minPoints, 运行时间: $runtime 秒, 聚类个数: $clusterNumber
           |++++++++++""".stripMargin)
    }
  }

  /**
   * 不同数据量测试
   */
  def diffDataSize(eps: Double, minPoints: Int, maxPointsPerPartition: Int,
                   dataSizeList: Seq[Int], rootPath: String, method: String): Unit = {
    for (dataSize <- dataSizeList) {
      val data = load(s"$rootPath/$dataSize.txt")
      val tic = System.currentTimeMillis()
      val clusterNumber = method match {
        case "local-native" => DBSCANModel.localNativeDBScan(data, eps, minPoints)
        case "local-rtree" => DBSCANModel.localRTreeDBScan(data, eps, minPoints)
        case "mr" => DBSCANModel.distributedDBScan(data, eps, minPoints, maxPointsPerPartition)
      }
      val tok = System.currentTimeMillis()
      val runtime = ((tok - tic) / 1000.0).formatted("%.2f")
      println(
        s"""++++++++++
           |[不同数据量测试: eps=$eps, minPoints=$minPoints, maxPointPerPartition=$maxPointsPerPartition, rootPath=$rootPath, method=$method]
           |数据量: $dataSize, 运行时间: $runtime 秒, 聚类个数: $clusterNumber
           |++++++++++""".stripMargin)
    }
  }

  /**
   * 不同分区内最大点数
   */
  def diffMaxPointsPerPartition(eps: Double, minPoints: Int, maxPointsPerPartitionList: Seq[Int],
                                dataSize: Int, rootPath: String): Unit = {
    for (maxPointsPerPartition <- maxPointsPerPartitionList) {
      val data = load(s"$rootPath/$dataSize.txt")
      val tic = System.currentTimeMillis()
      val clusterNumber = DBSCANModel.distributedDBScan(data, eps, minPoints, maxPointsPerPartition)
      val tok = System.currentTimeMillis()
      val runtime = ((tok - tic) / 1000.0).formatted("%.2f")
      println(
        s"""++++++++++
           |[不同分区内最大点数测试: eps=$eps, minPoints=$minPoints, maxPointPerPartition=$maxPointsPerPartition, rootPath=$rootPath, method=mr]
           |分区内最大点数: $maxPointsPerPartition, 运行时间: $runtime 秒, 聚类个数: $clusterNumber
           |++++++++++""".stripMargin)
    }
  }


  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DBSCAN-EXP")
    sc = new SparkContext(conf)

    val op = args(0).toLowerCase
    println(
      s"""++++++++++
         |测试类型: [$op]
         |++++++++++""".stripMargin)

    op match {
      case "diff-data-size" =>
        val (eps, minPoints, maxPointsPerPartition, dataSizeList, rootPath, method) =
          (args(1).toDouble, args(2).toInt, args(3).toInt, args(4).split(",").map(_.toInt), args(5), args(6))
        diffDataSize(eps, minPoints, maxPointsPerPartition, dataSizeList, rootPath, method)
      case "diff-eps" =>
        val (epsList, minPoints, maxPointsPerPartition, dataSize, rootPath, method) =
          (args(1).split(",").map(_.toDouble), args(2).toInt, args(3).toInt, args(4).toInt, args(5), args(6))
        diffEps(epsList, minPoints, maxPointsPerPartition, dataSize, rootPath, method)
      case "diff-min-points" =>
        val (eps, minPointsList, maxPointsPerPartition, dataSize, rootPath, method) =
          (args(1).toDouble, args(2).split(",").map(_.toInt), args(3).toInt, args(4).toInt, args(5), args(6))
        diffMinPoints(eps, minPointsList, maxPointsPerPartition, dataSize, rootPath, method)
      case "diff-max-points-per-partition" =>
        val (eps, minPoints, maxPointsPerPartitionList, dataSize, rootPath) =
          (args(1).toDouble, args(2).toInt, args(3).split(",").map(_.toInt), args(4).toInt, args(5))
        diffMaxPointsPerPartition(eps, minPoints, maxPointsPerPartitionList, dataSize, rootPath)
      case _ => throw new IllegalArgumentException(s"暂不支持该操作: $op")
    }
  }
}
