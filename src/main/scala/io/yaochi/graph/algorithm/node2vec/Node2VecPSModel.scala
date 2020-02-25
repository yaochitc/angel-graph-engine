package io.yaochi.graph.algorithm.node2vec

import java.util.{ArrayList => JArrayList}

import com.tencent.angel.graph.data.Node
import com.tencent.angel.ml.matrix.psf.update.XavierUniform
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSMatrix
import com.tencent.angel.spark.models.impl.PSMatrixImpl
import io.yaochi.graph.algorithm.base.UnsupervisedGNNPSModel
import io.yaochi.graph.optim.AsyncOptim
import org.apache.spark.rdd.RDD

class Node2VecPSModel(graph: PSMatrix,
                      val embeddingDim: Int,
                      val embedding: PSMatrix) extends UnsupervisedGNNPSModel(graph) {
  override def initialize(): Unit = {
    embedding.psfUpdate(new XavierUniform(embedding.id, 0, embeddingDim, 1.0,
      embedding.rows, embedding.columns)).get()
  }
}

object Node2VecPSModel {
  def apply(minId: Long, maxId: Long, embeddingDim: Int, optim: AsyncOptim,
            index: RDD[Long], psNumPartition: Int,
            useBalancePartition: Boolean = false): Node2VecPSModel = {
    val graph = new MatrixContext("graph", 1, minId, maxId)
    graph.setRowType(RowType.T_ANY_LONGKEY_SPARSE)
    graph.setValueType(classOf[Node])

    if (useBalancePartition)
      LoadBalancePartitioner.partition(index, maxId, psNumPartition, graph)

    val embedding = new MatrixContext("embedding", embeddingDim * optim.getNumSlots(), minId, maxId)

    val list = new JArrayList[MatrixContext]()
    list.add(graph)
    list.add(embedding)

    PSAgentContext.get().getMasterClient.createMatrices(list, 10000L)
    val graphId = PSAgentContext.get().getMasterClient.getMatrix("graph").getId
    val embeddingId = PSAgentContext.get().getMasterClient.getMatrix("embedding").getId

    new Node2VecPSModel(new PSMatrixImpl(graphId, 1, maxId, graph.getRowType),
      embeddingDim,
      new PSMatrixImpl(embeddingId, 0, maxId, embedding.getRowType)
    )
  }
}