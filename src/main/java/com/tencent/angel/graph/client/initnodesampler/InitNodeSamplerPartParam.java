package com.tencent.angel.graph.client.initnodesampler;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class InitNodeSamplerPartParam extends PartitionUpdateParam {

	private int[] index;
	private long[] nodeIds;
	private int[] types;
	private float[] weights;
	private int startIndex;
	private int endIndex;

	public InitNodeSamplerPartParam(int matrixId, PartitionKey pkey,
									int[] index, long[] nodeIds, int[] types,
									float[] weights, int startIndex, int endIndex) {
		super(matrixId, pkey);
		this.index = index;
		this.nodeIds = nodeIds;
		this.types = types;
		this.weights = weights;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
	}

	public InitNodeSamplerPartParam() {
		this(0, null,null, null, null, null, 0, 0);
	}

	public long[] getNodeIds() {
		return nodeIds;
	}

	public int[] getTypes() {
		return types;
	}

	public float[] getWeights() {
		return weights;
	}

	@Override
	public void serialize(ByteBuf buf) {
		super.serialize(buf);

		buf.writeInt(endIndex - startIndex);
		for (int i = startIndex; i < endIndex; i++) {
			long nodeId = nodeIds[index[i]];
			int type = types[index[i]];
			float weight = weights[index[i]];
			buf.writeLong(nodeId);
			buf.writeInt(type);
			buf.writeFloat(weight);
		}
	}

	@Override
	public void deserialize(ByteBuf buf) {
		super.deserialize(buf);

		int size = buf.readInt();
		nodeIds = new long[size];
		types = new int[size];
		weights = new float[size];

		for (int i = 0; i < size; i++) {
			long node = buf.readLong();
			int type = buf.readInt();
			float weight = buf.readFloat();
			nodeIds[i] = node;
			types[i] = type;
			weights[i] = weight;
		}

	}

	@Override
	public int bufferLen() {
		int len = super.bufferLen();
		len += 4;
		for (int i = startIndex; i < endIndex; i++) {
			len += 16;
		}
		return len;
	}
}