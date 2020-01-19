package io.yaochi.graph.algorithm.base

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import io.yaochi.graph.dataset.CoraDataset
import io.yaochi.graph.util.DataLoaderUtils
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
    val psModel = GNNPSModel(minId, maxId, index, 1)
  }

  @After
  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
