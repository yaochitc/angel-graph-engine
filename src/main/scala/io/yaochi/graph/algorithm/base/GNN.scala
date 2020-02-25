package io.yaochi.graph.algorithm.base

import com.tencent.angel.spark.ml.graph.params._
import io.yaochi.graph.data.SampleParser
import io.yaochi.graph.params._
import io.yaochi.graph.spark.RangePartitioner
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}

abstract class GNN[PSModel <: GNNPSModel, Model <: GNNModel](val uid: String) extends Serializable
  with HasBatchSize with HasFeatureDim with HasOptimizer
  with HasNumEpoch with HasNumSamples with HasNumBatchInit
  with HasPartitionNum with HasPSPartitionNum with HasUseBalancePartition
  with HasDataFormat with HasStorageLevel {

  def this() = this(Identifiable.randomUID("GNN"))

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  def initFeatures(model: PSModel, features: DataFrame, minId: Long, maxId: Long): Unit = {
    features.select("id", "feature")
      .rdd.map(row => (row.getLong(0), row.getString(1)))
      .filter(f => f._1 >= minId && f._1 <= maxId)
      .map(f => (f._1, SampleParser.parseFeature(f._2, $(featureDim), $(dataFormat))))
      .mapPartitionsWithIndex((_, it) =>
        Iterator(NodeFeaturePartition.apply(it)))
      .map(_.init(model, $(numBatchInit))).count()
  }

  def makeEdges(edgeDF: DataFrame, hasType: Boolean, hasWeight: Boolean): RDD[Edge] = {
    val edges = (hasType, hasWeight) match {
      case (false, false) =>
        edgeDF.select("src", "dst").rdd
          .map(row => Edge(row.getLong(0), row.getLong(1), None, None))
      case (true, false) =>
        edgeDF.select("src", "dst", "type").rdd
          .map(row => Edge(row.getLong(0), row.getLong(1), Some(row.getInt(2)), None))
      case (false, true) =>
        edgeDF.select("src", "dst", "weight").rdd
          .map(row => Edge(row.getLong(0), row.getLong(1), None, Some(row.getFloat(2))))
      case (true, true) =>
        edgeDF.select("src", "dst", "type", "weight").rdd
          .map(row => Edge(row.getLong(0), row.getLong(1), Some(row.getInt(2)), Some(row.getFloat(1))))
    }
    edges.filter(f => f.src != f.dst)
  }

  def makeGlobalNodeModel(featureDF: DataFrame, minId: Long, maxId: Long, hasType: Boolean, hasWeight: Boolean, numTypes: Int): GNNNodeSamplerModel = {
    val nodes = (hasType, hasWeight) match {
      case (false, false) =>
        featureDF.select("id").rdd
          .map(row => Node(row.getLong(0), None, None))
      case (true, false) =>
        featureDF.select("id", "weight").rdd
          .map(row => Node(row.getLong(0), Some(row.getInt(1)), None))
      case (false, true) =>
        featureDF.select("id", "type").rdd
          .map(row => Node(row.getLong(0), None, Some(row.getFloat(1))))
      case (true, true) =>
        featureDF.select("id", "type", "weight").rdd
          .map(row => Node(row.getLong(0), Some(row.getInt(1)), Some(row.getFloat(2))))
    }

    val index = nodes.map(_.id)
    val nodeSamplerModel = GNNNodeSamplerModel(minId, maxId, index, hasType, hasWeight, numTypes, $(psPartitionNum))

    val partitonedNodes = nodes.map(node => (node.id, node))
      .partitionBy(RangePartitioner(nodeSamplerModel.matrix.id))
      .map(_._2)

    partitonedNodes.persist($(storageLevel))
    partitonedNodes.foreachPartition(_ => Unit)

    nodeSamplerModel
  }

  def initialize(edgeDF: DataFrame,
                 featureDF: DataFrame): (Model, PSModel, Dataset[_])

  def fit(model: Model, psModel: PSModel, graph: Dataset[_]): Unit

}
