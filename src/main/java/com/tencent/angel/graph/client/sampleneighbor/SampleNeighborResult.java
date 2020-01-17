package com.tencent.angel.graph.client.sampleneighbor;

import com.tencent.angel.graph.data.Neighbor;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class SampleNeighborResult extends GetResult {

	/**
	 * Node id to neighbors map
	 */
	private Long2ObjectOpenHashMap<Neighbor> nodeIdToNeighbors;

	SampleNeighborResult(Long2ObjectOpenHashMap<Neighbor> nodeIdToNeighbors) {
		this.nodeIdToNeighbors = nodeIdToNeighbors;
	}

	public Long2ObjectOpenHashMap<Neighbor> getNodeIdToNeighbors() {
		return nodeIdToNeighbors;
	}

	public void setNodeIdToNeighbors(
			Long2ObjectOpenHashMap<Neighbor> nodeIdToNeighbors) {
		this.nodeIdToNeighbors = nodeIdToNeighbors;
	}
}
