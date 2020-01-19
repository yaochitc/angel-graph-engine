package io.yaochi.graph.algorithm.base

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import io.yaochi.graph.dataset.CoraDataset
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
    val (edges, features, labels) = CoraDataset.load("data/cora")
  }

  @After
  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
