package io.yaochi.graph.algorithm.base

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import io.yaochi.graph.dataset.CoraDataset
import io.yaochi.graph.util.DataLoaderUtils
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

  @Test
  def testGNNPSModel(): Unit = {
    val (edgeDF, featureDF, labelDF) = CoraDataset.load("data/cora")

    val edges = edgeDF.select("src", "dst").rdd
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(f => f._1 != f._2)
    val (minId, maxId, numEdges) = edges.mapPartitions(DataLoaderUtils.summarizeApplyOp)
      .reduce(DataLoaderUtils.summarizeReduceOp)
    val index = edges.flatMap(f => Iterator(f._1, f._2))
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges")

    PSContext.getOrCreate(SparkContext.getOrCreate())

    val partitionNum = 1
    val numBatchInit = 4
    val storageLevel = StorageLevel.DISK_ONLY

    val psModel = GNNPSModel(minId, maxId + 1, index, partitionNum)
    val adj = edges.groupByKey(partitionNum)

    val adjGraph = adj.mapPartitionsWithIndex((index, it) =>
      Iterator.single(GraphAdjPartition(index, it)))

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
