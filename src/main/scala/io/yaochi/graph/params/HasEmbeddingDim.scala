package io.yaochi.graph.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasEmbeddingDim extends Params {

  final val embeddingDim = new IntParam(this, "embeddingSize", "embeddingSize")

  final def getEmbeddingDim: Int = $(embeddingDim)

  setDefault(embeddingDim, 10)

  final def setEmbeddingDim(dim: Int): this.type = set(embeddingDim, dim)

}
