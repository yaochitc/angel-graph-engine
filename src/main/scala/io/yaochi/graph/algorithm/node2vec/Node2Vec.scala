package io.yaochi.graph.algorithm.node2vec

import com.tencent.angel.spark.context.PSContext
import io.yaochi.graph.algorithm.base.{Edge, GNN, GraphAdjPartition, Node}
import io.yaochi.graph.params.HasEmbeddingDim
import io.yaochi.graph.util.DataLoaderUtils
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

class Node2Vec extends GNN[Node2VecPSModel, Node2VecModel]
  with HasEmbeddingDim {

  def makeModel(): Node2VecModel = Node2VecModel()

  def makePSModel(minId: Long, maxId: Long, index: RDD[Long], model: Node2VecModel): Node2VecPSModel =  {
    Node2VecPSModel.apply(minId, maxId, $(embeddingDim), getOptimizer,
      index, $(psPartitionNum), $(useBalancePartition))
  }

  def makeGraph(edges: RDD[Edge], model: Node2VecPSModel, hasWeight: Boolean): Dataset[_] = {
    // build adj graph partitions
    val adjGraph = edges.map(f => (f.src, f)).groupByKey($(partitionNum))
      .mapPartitionsWithIndex((index, it) =>
        Iterator.single(GraphAdjPartition.apply(index, it, hasType = false, hasWeight)))

    adjGraph.persist($(storageLevel))
    adjGraph.foreachPartition(_ => Unit)
    adjGraph.map(_.init(model, $(numBatchInit))).reduce(_ + _)

    model.initNeighborSampler(0, hasWeight)

    // build Node2Vec graph partitions
    val node2vecGraph = adjGraph.map(Node2VecPartition(_))
    node2vecGraph.persist($(storageLevel))
    node2vecGraph.count()
    adjGraph.unpersist(true)

    implicit val encoder = org.apache.spark.sql.Encoders.kryo[Node2VecPartition]
    SparkSession.builder().getOrCreate().createDataset(node2vecGraph)
  }

  override def initialize(edgeDF: DataFrame, featureDF: DataFrame): (Node2VecModel, Node2VecPSModel, Dataset[_]) = {
    val start = System.currentTimeMillis()

    val columns = edgeDF.columns
    val hasWeight = columns.contains("weight")

    // read edges
    val edges = makeEdges(edgeDF, hasType = false, hasWeight)

    edges.persist(StorageLevel.DISK_ONLY)

    val (minId, maxId, numEdges) = edges.mapPartitions(DataLoaderUtils.summarizeApplyOp)
      .reduce(DataLoaderUtils.summarizeReduceOp)
    val index = edges.flatMap(f => Iterator(f.src, f.dst))
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges")

    PSContext.getOrCreate(SparkContext.getOrCreate())

    val model = makeModel()

    val psModel = makePSModel(minId, maxId + 1, index, model)
    psModel.initialize()

    val graph = makeGraph(edges, psModel, hasWeight)

    val end = System.currentTimeMillis()
    println(s"initialize cost ${(end - start) / 1000}s")

    (model, psModel, graph)
  }

  override def fit(model: Node2VecModel, PSModel: Node2VecPSModel, graph: Dataset[_]): Unit = {

  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}
