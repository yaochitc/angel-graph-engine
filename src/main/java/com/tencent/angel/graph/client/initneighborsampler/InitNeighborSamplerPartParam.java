package com.tencent.angel.graph.client.initneighborsampler;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class InitNeighborSamplerPartParam extends PartitionUpdateParam {

	private int numTypes;

	public InitNeighborSamplerPartParam(int matrixId, PartitionKey pkey, int numTypes) {
		super(matrixId, pkey);
		this.numTypes = numTypes;
	}

	public InitNeighborSamplerPartParam() {
		this(0, null, 0);
	}

	public int getNumTypes() {
		return numTypes;
	}

	@Override
	public void serialize(ByteBuf buf) {
		super.serialize(buf);
		buf.writeInt(numTypes);
	}

	@Override
	public void deserialize(ByteBuf buf) {
		super.deserialize(buf);
		numTypes = buf.readInt();
	}

	@Override
	public int bufferLen() {
		int len = super.bufferLen();
		return len + 4;
	}
}