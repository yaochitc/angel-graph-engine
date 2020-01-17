package com.tencent.angel.graph.data;

import com.tencent.angel.common.Serialize;
import io.netty.buffer.ByteBuf;

public class Neighbor implements Serialize {

	private long[] nodeIds;
	private float[] weights;

	public Neighbor(long[] nodeIds, float[] weights) {
		this.nodeIds = nodeIds;
		this.weights = weights;
	}

	public Neighbor() {
		this(null, null);
	}

	public long[] getNodeIds() {
		return nodeIds;
	}

	public void setNodeIds(long[] nodeIds) {
		this.nodeIds = nodeIds;
	}

	public float[] getWeights() {
		return weights;
	}

	public void setWeights(float[] weights) {
		this.weights = weights;
	}

	public static Neighbor empty() {
		return new Neighbor(new long[0], null);
	}

	@Override
	public void serialize(ByteBuf output) {
		output.writeInt(nodeIds.length);
		for (int i = 0; i < nodeIds.length; i++)
			output.writeLong(nodeIds[i]);

		if (weights != null) {
			output.writeInt(weights.length);
			for (int i = 0; i < weights.length; i++)
				output.writeFloat(weights[i]);
		}
	}

	@Override
	public void deserialize(ByteBuf input) {
		int len = input.readInt();
		nodeIds = new long[len];
		for (int i = 0; i < len; i++)
			nodeIds[i] = input.readLong();

		if (weights != null) {
			len = input.readInt();
			weights = new float[len];
			for (int i = 0; i < len; i++)
				weights[i] = input.readFloat();
		}
	}

	@Override
	public int bufferLen() {
		int len = 4 + 8 * nodeIds.length;
		if (weights != null)
			len += 4 + 4 * weights.length;
		return len;
	}
}
