package com.tencent.angel.graph.client.initneighborsampler;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class InitNeighborSamplerPartParam extends PartitionUpdateParam {

	public InitNeighborSamplerPartParam(int matrixId, PartitionKey pkey) {
		super(matrixId, pkey);
	}

	public InitNeighborSamplerPartParam() {
		this(0, null);
	}

	@Override
	public void serialize(ByteBuf buf) {
		super.serialize(buf);
	}

	@Override
	public void deserialize(ByteBuf buf) {
		super.deserialize(buf);
	}

	@Override
	public int bufferLen() {
		return super.bufferLen();
	}
}