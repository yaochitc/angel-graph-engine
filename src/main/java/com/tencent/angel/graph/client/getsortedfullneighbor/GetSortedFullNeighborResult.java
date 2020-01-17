package com.tencent.angel.graph.client.getsortedfullneighbor;

import com.tencent.angel.graph.data.Neighbor;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class GetSortedFullNeighborResult extends GetResult {

	/**
	 * Node id to neighbors map
	 */
	private Long2ObjectOpenHashMap<Neighbor> nodeIdToNeighbors;

	GetSortedFullNeighborResult(Long2ObjectOpenHashMap<Neighbor> nodeIdToNeighbors) {
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
