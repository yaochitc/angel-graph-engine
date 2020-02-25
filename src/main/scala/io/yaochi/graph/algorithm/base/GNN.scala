package io.yaochi.graph.algorithm.base

import com.tencent.angel.spark.ml.graph.params._
import io.yaochi.graph.data.SampleParser
import io.yaochi.graph.params._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}

abstract class GNN[PSModel <: GNNPSModel, Model <: GNNModel](val uid: String) extends Serializable
  with HasBatchSize with HasFeatureDim with HasOptimizer
  with HasNumEpoch with HasNumSamples with HasNumBatchInit
  with HasPartitionNum with HasPSPartitionNum with HasUseBalancePartition
  with HasDataFormat with HasStorageLevel {

  def this() = this(Identifiable.randomUID("GNN"))

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  def initFeatures(model: PSModel, features: Dataset[Row], minId: Long, maxId: Long): Unit = {
    features.rdd.filter(row => row.length > 0)
      .filter(row => row.get(0) != null)
      .map(row => (row.getLong(0), row.getString(1)))
      .filter(f => f._1 >= minId && f._1 <= maxId)
      .map(f => (f._1, SampleParser.parseFeature(f._2, $(featureDim), $(dataFormat))))
      .mapPartitionsWithIndex((index, it) =>
        Iterator(NodeFeaturePartition.apply(index, it)))
      .map(_.init(model, $(numBatchInit))).count()
  }

  def makeModel(): Model

  def makePSModel(minId: Long, maxId: Long, index: RDD[Long], model: Model): PSModel

  def makeGraph(edges: RDD[Edge], model: PSModel, hasType: Boolean, hasWeight: Boolean): Dataset[_]

  def makeEdges(edgeDF: DataFrame, hasType: Boolean, hasWeight: Boolean): RDD[Edge] = {
    val edges = (hasType, hasWeight) match {
      case (false, false) =>
        edgeDF.select("src", "dst").rdd
          .map(row => Edge(row.getLong(0), row.getLong(1), None, None))
      case (true, false) =>
        edgeDF.select("src", "dst", "weight").rdd
          .map(row => Edge(row.getLong(0), row.getLong(1), Some(row.getInt(2)), None))
      case (false, true) =>
        edgeDF.select("src", "dst", "type").rdd
          .map(row => Edge(row.getLong(0), row.getLong(1), None, Some(row.getFloat(2))))
      case (true, true) =>
        edgeDF.select("src", "dst", "weight", "type").rdd
          .map(row => Edge(row.getLong(0), row.getLong(1), Some(row.getInt(2)), Some(row.getLong(1))))
    }
    edges.filter(f => f.src != f.dst)
  }

  def initialize(edgeDF: DataFrame,
                 featureDF: DataFrame): (Model, PSModel, Dataset[_])

  def fit(model: Model, psModel: PSModel, graph: Dataset[_]): Unit

}
