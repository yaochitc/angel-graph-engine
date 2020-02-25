package io.yaochi.graph.algorithm.node2vec

import io.yaochi.graph.algorithm.base.{GNNPartition, GraphAdjPartition}
import io.yaochi.graph.optim.AsyncOptim

class Node2VecPartition(index: Int,
                        keys: Array[Long],
                        indptr: Array[Int],
                        neighbors: Array[Long]) extends
  GNNPartition[Node2VecPSModel, Node2VecModel](index, keys, indptr, neighbors) {

  override def trainEpoch(curEpoch: Int,
                          batchSize: Int,
                          model: Node2VecModel,
                          psModel: Node2VecPSModel,
                          featureDim: Int,
                          optim: AsyncOptim,
                          numSample: Int): (Double, Long) = {
    null
  }

}

object Node2VecPartition {
  def apply(adjPartition: GraphAdjPartition): Node2VecPartition = {
    new Node2VecPartition(adjPartition.index,
      adjPartition.keys,
      adjPartition.indptr,
      adjPartition.neighbours)
  }
}