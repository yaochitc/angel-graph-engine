package com.tencent.angel.graph.client.initneighborsampler;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class InitNeighborSamplerPartParam extends PartitionUpdateParam {

	private int numTypes;
	private boolean hasWeight;

	public InitNeighborSamplerPartParam(int matrixId, PartitionKey pkey, int numTypes, boolean hasWeight) {
		super(matrixId, pkey);
		this.numTypes = numTypes;
		this.hasWeight = hasWeight;
	}

	public InitNeighborSamplerPartParam() {
		this(0, null, 0, false);
	}

	public int getNumTypes() {
		return numTypes;
	}

	public boolean isHasWeight() {
		return hasWeight;
	}

	@Override
	public void serialize(ByteBuf buf) {
		super.serialize(buf);
		buf.writeInt(numTypes);
		buf.writeBoolean(hasWeight);
	}

	@Override
	public void deserialize(ByteBuf buf) {
		super.deserialize(buf);
		numTypes = buf.readInt();
		hasWeight = buf.readBoolean();
	}

	@Override
	public int bufferLen() {
		int len = super.bufferLen();
		return len + 5;
	}
}