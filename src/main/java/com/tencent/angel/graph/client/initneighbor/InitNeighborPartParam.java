package com.tencent.angel.graph.client.initneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class InitNeighborPartParam extends PartitionUpdateParam {

	private long[] keys;
	private int[] index;
	private int[] indptr;
	private long[] neighbors;
	private int[] types;
	private float[] weights;
	private int startIndex;
	private int endIndex;
	private long[][] neighborArrays;
	private int[][] typeArrays;
	private float[][] weightArrays;

	public InitNeighborPartParam(int matrixId, PartitionKey pkey,
								 long[] keys, int[] index, int[] indptr,
								 long[] neighbors, int[] types, float[] weights,
								 int startIndex, int endIndex) {
		super(matrixId, pkey);
		this.keys = keys;
		this.index = index;
		this.indptr = indptr;
		this.neighbors = neighbors;
		this.types = types;
		this.weights = weights;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
	}

	public InitNeighborPartParam() {
		this(0, null, null, null, null, null, null, null, 0, 0);
	}

	public long[] getKeys() {
		return keys;
	}

	public long[][] getNeighborArrays() {
		return neighborArrays;
	}

	public int[][] getTypeArrays() {
		return typeArrays;
	}

	public float[][] getWeightArrays() {
		return weightArrays;
	}

	@Override
	public void serialize(ByteBuf buf) {
		super.serialize(buf);
		if (types != null)
			buf.writeBoolean(true);
		else
			buf.writeBoolean(false);

		if (weights != null)
			buf.writeBoolean(true);
		else
			buf.writeBoolean(false);

		buf.writeInt(endIndex - startIndex);
		for (int i = startIndex; i < endIndex; i++) {
			long key = keys[index[i]];
			int len = indptr[index[i] + 1] - indptr[index[i]];
			buf.writeLong(key);
			buf.writeInt(len);
			for (int j = indptr[index[i]]; j < indptr[index[i] + 1]; j++)
				buf.writeLong(neighbors[j]);
			if (types != null) {
				for (int j = indptr[index[i]]; j < indptr[index[i] + 1]; j++)
					buf.writeInt(types[j]);
			}
			if (weights != null) {
				for (int j = indptr[index[i]]; j < indptr[index[i] + 1]; j++)
					buf.writeFloat(weights[j]);
			}
		}
	}

	@Override
	public void deserialize(ByteBuf buf) {
		super.deserialize(buf);
		boolean hasType = buf.readBoolean();
		boolean hasWeight = buf.readBoolean();

		int size = buf.readInt();
		keys = new long[size];
		neighborArrays = new long[size][];

		if (hasType)
			typeArrays = new int[size][];

		if (hasWeight)
			weightArrays = new float[size][];

		for (int i = 0; i < size; i++) {
			long node = buf.readLong();
			keys[i] = node;
			int len = buf.readInt();
			long[] neighbors = new long[len];
			for (int j = 0; j < len; j++)
				neighbors[j] = buf.readLong();
			neighborArrays[i] = neighbors;

			if (hasType) {
				int[] types = new int[len];
				for (int j = 0; j < len; j++)
					types[j] = buf.readInt();
				typeArrays[i] = types;
			}

			if (hasWeight) {
				float[] weights = new float[len];
				for (int j = 0; j < len; j++)
					weights[j] = buf.readFloat();
				weightArrays[i] = weights;
			}
		}
	}

	@Override
	public int bufferLen() {
		int len = super.bufferLen();
		len += 4;
		for (int i = startIndex; i < endIndex; i++) {
			len += 12;
			len += 8 * (indptr[index[i] + 1] - indptr[index[i]]);
			if (types != null)
				len += 4 * (indptr[index[i] + 1] - indptr[index[i]]);
			if (weights != null)
				len += 4 * (indptr[index[i] + 1] - indptr[index[i]]);
		}
		return len;
	}
}