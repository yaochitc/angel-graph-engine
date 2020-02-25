package io.yaochi.graph.algorithm.node2vec

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import io.yaochi.graph.dataset.{ArxivDataset, CoraDataset}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class Node2VecTest {
  @Before
  def start(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("node2vec")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("cp")
  }

  @Test
  def testnode2vec(): Unit = {
    val node2vec = new Node2Vec()
    node2vec.setDataFormat("dense")
    node2vec.setFeatureDim(1433)
    node2vec.setOptimizer("adam")
    node2vec.setUseBalancePartition(false)
    node2vec.setBatchSize(100)
    node2vec.setStepSize(0.02)
    node2vec.setPSPartitionNum(10)
    node2vec.setPartitionNum(1)
    node2vec.setUseBalancePartition(false)
    node2vec.setNumEpoch(100)
    node2vec.setStorageLevel("MEMORY_ONLY")

    val edges = ArxivDataset.load("data/arXiv")
    val (model, psModel, graph) = node2vec.initialize(edges, null)
    node2vec.fit(model, psModel, graph)

  }

  @After
  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
