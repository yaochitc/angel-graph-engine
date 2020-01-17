package com.tencent.angel.graph.client.getfullneighbor;

import com.tencent.angel.graph.data.Neighbor;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class GetFullNeighborResult extends GetResult {

	/**
	 * Node id to neighbors map
	 */
	private Long2ObjectOpenHashMap<Neighbor> nodeIdToNeighbors;

	GetFullNeighborResult(Long2ObjectOpenHashMap<Neighbor> nodeIdToNeighbors) {
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
