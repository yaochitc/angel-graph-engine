package io.yaochi.graph.algorithm.base

import com.tencent.angel.graph.client.getfullneighbor.{GetFullNeighbor, GetFullNeighborParam, GetFullNeighborResult}
import com.tencent.angel.graph.client.getnodefeats.{GetNodeFeats, GetNodeFeatsParam, GetNodeFeatsResult}
import com.tencent.angel.graph.client.initneighbor.{InitNeighbor, InitNeighborParam}
import com.tencent.angel.graph.client.initneighborsampler.{InitNeighborSampler, InitNeighborSamplerParam}
import com.tencent.angel.graph.client.initnodefeats.{InitNodeFeats, InitNodeFeatsParam}
import com.tencent.angel.graph.client.sampleneighbor.{SampleNeighbor, SampleNeighborParam, SampleNeighborResult}
import com.tencent.angel.graph.data.Neighbor
import com.tencent.angel.ml.math2.vector.IntFloatVector
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

abstract class GNNPSModel(val graph: PSMatrix) extends Serializable {

  def initialize(): Unit = {
  }

  def initNeighbors(keys: Array[Long],
                    indptr: Array[Int],
                    neighbors: Array[Long],
                    types: Array[Int],
                    weights: Array[Float],
                    numBatch: Int): Unit = {
    println(s"mini batch init neighbors")
    val step = keys.length / numBatch
    assert(step > 0)
    var start = 0
    while (start < keys.length) {
      val end = math.min(start + step, keys.length)
      initNeighbors(keys, indptr, neighbors, types, weights, start, end)
      start += step
    }
  }

  def initNeighbors(keys: Array[Long],
                    indptr: Array[Int],
                    neighbors: Array[Long],
                    types: Array[Int],
                    weights: Array[Float],
                    start: Int,
                    end: Int): Unit = {
    val param = new InitNeighborParam(graph.id, keys, indptr, neighbors, types, weights, start, end)
    val func = new InitNeighbor(param)
    graph.psfUpdate(func).get()
  }

  def initNodeFeatures(keys: Array[Long], features: Array[IntFloatVector],
                       numBatch: Int): Unit = {
    println(s"mini batch init features")
    println(s"keys.length=${keys.length} numBatch=$numBatch")
    val step = keys.length / numBatch
    assert(step > 0)
    var start = 0
    while (start < keys.length) {
      val end = math.min(start + step, keys.length)
      initNodeFeatures(keys, features, start, end)
      start += step
    }
  }

  def initNodeFeatures(keys: Array[Long], features: Array[IntFloatVector],
                       start: Int, end: Int): Unit = {
    val param = new InitNodeFeatsParam(graph.id, keys, features, start, end)
    val func = new InitNodeFeats(param)
    graph.psfUpdate(func).get()
  }

  def initNeighborSampler(numTypes: Int): Unit = {
    val param = new InitNeighborSamplerParam(graph.id, numTypes)
    val func = new InitNeighborSampler(param)
    graph.psfUpdate(func).get()
  }

  def getFeatures(keys: Array[Long]): Long2ObjectOpenHashMap[IntFloatVector] = {
    val func = new GetNodeFeats(new GetNodeFeatsParam(graph.id, keys.clone()))
    graph.psfGet(func).asInstanceOf[GetNodeFeatsResult].getResult
  }

  def getFullNeighbors(keys: Array[Long], types: Array[Int]): Long2ObjectOpenHashMap[Neighbor] = {
    val func = new GetFullNeighbor(new GetFullNeighborParam(graph.id, keys.clone(), types))
    graph.psfGet(func).asInstanceOf[GetFullNeighborResult].getNodeIdToNeighbors
  }

  def sampleNeighbors(keys: Array[Long], types: Array[Int], count: Int): Long2ObjectOpenHashMap[Neighbor] = {
    val func = new SampleNeighbor(new SampleNeighborParam(graph.id, keys.clone(), types, count))
    graph.psfGet(func).asInstanceOf[SampleNeighborResult].getNodeIdToNeighbors
  }
}
