package io.yaochi.graph.algorithm.base

import com.tencent.angel.graph.client.initnodesampler.{InitNodeSampler, InitNodeSamplerParam}
import com.tencent.angel.graph.client.samplenode.{SampleNode, SampleNodeParam, SampleNodeResult}
import com.tencent.angel.graph.data.NodeSample
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSMatrix
import com.tencent.angel.spark.models.impl.PSMatrixImpl
import org.apache.spark.rdd.RDD

class GNNNodeSamplerModel(val matrix: PSMatrix) {
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
    val param = new InitNodeSamplerParam(matrix.id, nodeIds, types, weights, start, end)
    val func = new InitNodeSampler(param)
    matrix.psfUpdate(func).get()
  }

  def sampleNodes(typeGroupIndexArray: Array[Array[Int]], countArray: Array[Array[Int]]): Array[Long] = {
    val param = new SampleNodeParam(matrix.id, typeGroupIndexArray, countArray)
    val func = new SampleNode(param)
    matrix.psfGet(func).asInstanceOf[SampleNodeResult].getNodeIds
  }
}

object GNNNodeSamplerModel {
  def apply(minId: Long,
            maxId: Long,
            index: RDD[Long],
            hasType: Boolean,
            hasWeight: Boolean,
            numTypes: Int,
            psNumPartition: Int,
            useBalancePartition: Boolean = false): GNNNodeSamplerModel = {
    val matrix = new MatrixContext("nodeSampler", 1, minId, maxId)
    matrix.setRowType(RowType.T_ANY_LONGKEY_SPARSE)
    matrix.setValueType(classOf[NodeSample])

    if (useBalancePartition)
      LoadBalancePartitioner.partition(index, maxId, psNumPartition, matrix)

    PSAgentContext.get().getMasterClient.createMatrix(matrix, 10000L)
    val matrixId = PSAgentContext.get().getMasterClient.getMatrix("nodeSampler").getId

    new GNNNodeSamplerModel(new PSMatrixImpl(matrixId, 1, maxId, matrix.getRowType))
  }
}