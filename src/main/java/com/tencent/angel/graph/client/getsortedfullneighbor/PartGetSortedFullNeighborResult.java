package com.tencent.angel.graph.client.getsortedfullneighbor;

import com.tencent.angel.graph.data.Neighbor;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartGetSortedFullNeighborResult extends PartitionGetResult {

	private int partId;
	/**
	 * Node id to neighbors map
	 */
	private Neighbor[] nodeIdToNeighbors;
	private static final int NullMark = 0;
	private static final int NotNullMark = 1;

	public PartGetSortedFullNeighborResult(int partId, Neighbor[] nodeIdToNeighbors) {
		this.partId = partId;
		this.nodeIdToNeighbors = nodeIdToNeighbors;
	}

	public PartGetSortedFullNeighborResult() {
		this(-1, null);
	}

	public Neighbor[] getNodeIdToNeighbors() {
		return nodeIdToNeighbors;
	}

	public void setNodeIdToNeighbors(
			Neighbor[] nodeIdToNeighbors) {
		this.nodeIdToNeighbors = nodeIdToNeighbors;
	}

	public int getPartId() {
		return partId;
	}

	@Override
	public void serialize(ByteBuf output) {
		output.writeInt(partId);
		output.writeInt(nodeIdToNeighbors.length);
		for (int i = 0; i < nodeIdToNeighbors.length; i++) {
			if (nodeIdToNeighbors[i] == null) {
				output.writeInt(NullMark);
			} else {
				output.writeInt(NotNullMark);
				nodeIdToNeighbors[i].serialize(output);
			}
		}
	}

	@Override
	public void deserialize(ByteBuf input) {
		partId = input.readInt();
		int size = input.readInt();
		nodeIdToNeighbors = new Neighbor[size];
		for (int i = 0; i < size; i++) {
			int mark = input.readInt();
			if (mark == NotNullMark) {
				nodeIdToNeighbors[i] = new Neighbor();
				nodeIdToNeighbors[i].deserialize(input);
			}
		}
	}

	@Override
	public int bufferLen() {
		int len = 8;
		for (int i = 0; i < nodeIdToNeighbors.length; i++) {
			len += 4;
			if (nodeIdToNeighbors[i] != null) {
				len += nodeIdToNeighbors[i].bufferLen();
			}
		}
		return len;
	}
}
