package io.yaochi.graph.algorithm.base

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import io.yaochi.graph.dataset.CoraDataset
import io.yaochi.graph.util.DataLoaderUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class GNNPSModelTest {
  @Before
  def start(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("gnn")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("cp")
  }

  def makeEdges(edgeDF: DataFrame, hasType: Boolean, hasWeight: Boolean): RDD[Edge] = {
    val edges = (hasType, hasWeight) match {
      case (false, false) =>
        edgeDF.select("src", "dst").rdd
          .map(row => Edge(row.getLong(0), row.getLong(1), None, None))
      case (true, false) =>
        edgeDF.select("src", "dst", "weight").rdd
          .map(row => Edge(row.getLong(0), row.getLong(1), Some(row.getInt(2)), None))
      case (false, true) =>
        edgeDF.select("src", "dst", "type").rdd
          .map(row => Edge(row.getLong(0), row.getLong(1), None, Some(row.getFloat(2))))
      case (true, true) =>
        edgeDF.select("src", "dst", "weight", "type").rdd
          .map(row => Edge(row.getLong(0), row.getLong(1), Some(row.getInt(2)), Some(row.getLong(1))))
    }
    edges.filter(f => f.src != f.dst)
  }

  @Test
  def testGNNPSModel(): Unit = {
    val (edgeDF, featureDF, labelDF) = CoraDataset.load("data/cora")

    val columns = edgeDF.columns
    val hasType = columns.contains("type")
    val hasWeight = columns.contains("weight")

    // read edges
    val edges = makeEdges(edgeDF, hasType, hasWeight)

    val (minId, maxId, numEdges) = edges.mapPartitions(DataLoaderUtils.summarizeApplyOp)
      .reduce(DataLoaderUtils.summarizeReduceOp)
    val index = edges.flatMap(f => Iterator(f.src, f.dst))
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges")

    PSContext.getOrCreate(SparkContext.getOrCreate())

    val partitionNum = 1
    val numBatchInit = 4
    val storageLevel = StorageLevel.DISK_ONLY

    val psModel = GNNPSModel(minId, maxId + 1, index, partitionNum)
    val adj = edges.map(f => (f.src, f)).groupByKey(partitionNum)

    val adjGraph = adj.mapPartitionsWithIndex((index, it) =>
      Iterator.single(GraphAdjPartition(index, it, hasType, hasWeight)))

    adjGraph.persist(storageLevel)
    adjGraph.foreachPartition(_ => Unit)
    adjGraph.map(_.init(psModel, numBatchInit)).reduce(_ + _)
  }

  @After
  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
