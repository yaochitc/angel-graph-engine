package io.yaochi.graph.spark

import java.util.{List => JList, TreeMap => JTreeMap}

import com.tencent.angel.ml.matrix.PartContext
import org.apache.spark.Partitioner

class RangePartitioner(parts: JList[PartContext]) extends Partitioner {
  private val id2Parts = RangePartitioner.buildId2Parts(parts)

  override def numPartitions:Int = id2Parts.size()

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Long]
    id2Parts.ceilingEntry(k).getValue
  }
}

object RangePartitioner {
  def apply(parts: JList[PartContext]): RangePartitioner = {
    new RangePartitioner(parts)
  }

  def buildId2Parts(parts: JList[PartContext]): JTreeMap[Long, Int] =  {
    val id2Parts = new JTreeMap[Long, Int]()
    for (i <- 0 until parts.size()) {
      val part = parts.get(i)
      id2Parts.put(part.getStartCol, i)
    }
    id2Parts
  }
}