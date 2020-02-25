package io.yaochi.graph.algorithm.node2vec

import com.tencent.angel.spark.context.PSContext
import io.yaochi.graph.algorithm.base.{Edge, GNN}
import io.yaochi.graph.util.DataLoaderUtils
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.storage.StorageLevel

class Node2Vec extends GNN[Node2VecPSModel, Node2VecModel] {

  def makeModel(): Node2VecModel = ???

  def makePSModel(minId: Long, maxId: Long, index: RDD[Long], model: Node2VecModel): Node2VecPSModel = ???

  def makeGraph(edges: RDD[Edge], model: Node2VecPSModel, hasWeight: Boolean, hasType: Boolean): Dataset[_] = ???

  override def initialize(edgeDF: DataFrame, featureDF: DataFrame): (Node2VecModel, Node2VecPSModel, Dataset[_]) = {
    val start = System.currentTimeMillis()

    val columns = edgeDF.columns
    val hasType = columns.contains("type")
    val hasWeight = columns.contains("weight")

    // read edges
    val edges = makeEdges(edgeDF, hasType, hasWeight)

    edges.persist(StorageLevel.DISK_ONLY)

    val (minId, maxId, numEdges) = edges.mapPartitions(DataLoaderUtils.summarizeApplyOp)
      .reduce(DataLoaderUtils.summarizeReduceOp)
    val index = edges.flatMap(f => Iterator(f.src, f.dst))
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges")

    PSContext.getOrCreate(SparkContext.getOrCreate())

    val model = makeModel()

    val psModel = makePSModel(minId, maxId + 1, index, model)
    psModel.initialize()

    val graph = makeGraph(edges, psModel, hasType, hasWeight)

    val end = System.currentTimeMillis()
    println(s"initialize cost ${(end - start) / 1000}s")

    (model, psModel, graph)
  }

  override def fit(model: Node2VecModel, PSModel: Node2VecPSModel, graph: Dataset[_]): Unit = {

  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}
