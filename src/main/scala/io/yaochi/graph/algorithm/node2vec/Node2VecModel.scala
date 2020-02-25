package io.yaochi.graph.algorithm.node2vec

import io.yaochi.graph.algorithm.base.GNNModel

class Node2VecModel extends GNNModel {

}

object Node2VecModel {
  def apply(): Node2VecModel = {
    new Node2VecModel()
  }
}