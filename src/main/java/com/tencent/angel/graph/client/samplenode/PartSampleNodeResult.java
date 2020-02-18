package com.tencent.angel.graph.client.samplenode;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartSampleNodeResult extends PartitionGetResult {

	private int partId;
	/**
	 * Node ids
	 */
	private long[] nodeIds;

	public PartSampleNodeResult(int partId, long[] nodeIds) {
		this.partId = partId;
		this.nodeIds = nodeIds;
	}

	public PartSampleNodeResult() {
		this(-1, null);
	}

	public long[] getNodeIds() {
		return nodeIds;
	}

	public void setNodeIds(
			long[] nodeIds) {
		this.nodeIds = nodeIds;
	}

	public int getPartId() {
		return partId;
	}

	@Override
	public void serialize(ByteBuf output) {
		output.writeInt(partId);
		output.writeInt(nodeIds.length);
		for (int i = 0; i < nodeIds.length; i++) {
			output.writeFloat(nodeIds[i]);
		}
	}

	@Override
	public void deserialize(ByteBuf input) {
		partId = input.readInt();
		int size = input.readInt();
		nodeIds = new long[input.readInt()];
		for (int i = 0; i < size; i++) {
			nodeIds[i] = input.readLong();
		}
	}

	@Override
	public int bufferLen() {
		return  8 + nodeIds.length;
	}
}
