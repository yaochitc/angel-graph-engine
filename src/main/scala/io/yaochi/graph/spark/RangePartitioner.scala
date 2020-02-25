package io.yaochi.graph.spark

import java.util.{TreeMap => JTreeMap}

import com.tencent.angel.psagent.PSAgentContext
import org.apache.spark.Partitioner

class RangePartitioner(matrixId: Int) extends Partitioner {
  private val id2Parts = RangePartitioner.buildId2Parts(matrixId)

  override def numPartitions:Int = id2Parts.size()

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Long]
    id2Parts.ceilingEntry(k).getValue
  }
}

object RangePartitioner {
  def apply(matrixId: Int): RangePartitioner = {
    new RangePartitioner(matrixId)
  }

  def buildId2Parts(matrixId: Int): JTreeMap[Long, Int] =  {
    val parts = PSAgentContext.get.getMatrixMetaManager.getPartitions(matrixId)
    val id2Parts = new JTreeMap[Long, Int]()
    for (i <- 0 until parts.size()) {
      val part = parts.get(i)
      id2Parts.put(part.getStartCol, part.getPartitionId)
    }
    id2Parts
  }
}