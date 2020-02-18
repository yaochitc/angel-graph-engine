package com.tencent.angel.graph.client.samplenode;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PartSampleNodeParam extends PartitionGetParam {

	private int[] typeGroupIndices;

	private int[] counts;

	public PartSampleNodeParam(int matrixId, PartitionKey part, int[] counts, int[] typeGroupIndices) {
		super(matrixId, part);
		this.typeGroupIndices = typeGroupIndices;
		this.counts = counts;
	}

	public PartSampleNodeParam() {
		this(0, null, null,null);
	}

	public int[] getTypeGroupIndices() {
		return typeGroupIndices;
	}

	public void setTypeGroupIndices(int[] typeGroupIndices) {
		this.typeGroupIndices = typeGroupIndices;
	}

	public int[] getCounts() {
		return counts;
	}

	public void setCounts(int[] counts) {
		this.counts = counts;
	}

	@Override
	public void serialize(ByteBuf buf) {
		super.serialize(buf);
		buf.writeInt(counts.length);
		for (int i = 0; i < counts.length; i++) {
			buf.writeInt(counts[i]);
		}

		buf.writeInt(typeGroupIndices.length);
		for (int i = 0; i < typeGroupIndices.length; i++) {
			buf.writeInt(typeGroupIndices[i]);
		}
	}

	@Override
	public void deserialize(ByteBuf buf) {
		super.deserialize(buf);
		counts = new int[buf.readInt()];
		for (int i = 0; i < counts.length; i++) {
			counts[i] = buf.readInt();
		}

		typeGroupIndices = new int[buf.readInt()];
		for (int i = 0; i < typeGroupIndices.length; i++) {
			typeGroupIndices[i] = buf.readInt();
		}
	}

	@Override
	public int bufferLen() {
		int len = super.bufferLen() + 8;
		len += 4 + counts.length + 4 * typeGroupIndices.length;
		return len;
	}
}
