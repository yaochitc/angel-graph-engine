package io.yaochi.graph.algorithm.base

import com.tencent.angel.graph.client.initnodesampler.{InitNodeSampler, InitNodeSamplerParam}
import com.tencent.angel.graph.client.samplenode.{SampleNode, SampleNodeParam, SampleNodeResult}
import com.tencent.angel.spark.models.PSMatrix

class UnsupervisedGNNPSModel(graph: PSMatrix) extends GNNPSModel(graph) {

  def initNodeSampler(nodeIds: Array[Long],
                      types: Array[Int],
                      weights: Array[Float],
                      numBatch: Int): Unit = {
    println(s"mini batch init node sampler")
    val step = nodeIds.length / numBatch
    assert(step > 0)
    var start = 0
    while (start < nodeIds.length) {
      val end = math.min(start + step, nodeIds.length)
      initNodeSampler(nodeIds, types, weights, start, end)
      start += step
    }
  }

  def initNodeSampler(nodeIds: Array[Long],
                      types: Array[Int],
                      weights: Array[Float],
                      start: Int,
                      end: Int): Unit = {
    val param = new InitNodeSamplerParam(graph.id, nodeIds, types, weights, start, end)
    val func = new InitNodeSampler(param)
    graph.psfUpdate(func).get()
  }

  def sampleNodes(typeGroupIndexArray: Array[Array[Int]], countArray: Array[Array[Int]]): Array[Long] = {
    val param = new SampleNodeParam(graph.id, typeGroupIndexArray, countArray)
    val func = new SampleNode(param)
    graph.psfGet(func).asInstanceOf[SampleNodeResult].getNodeIds
  }
}
