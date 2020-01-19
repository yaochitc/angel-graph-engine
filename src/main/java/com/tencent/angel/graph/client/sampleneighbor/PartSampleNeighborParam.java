package com.tencent.angel.graph.client.sampleneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PartSampleNeighborParam extends PartitionGetParam {

	private long[] nodeIds;

	private int[] types;

	private int count;

	private int startIndex;

	private int endIndex;

	public PartSampleNeighborParam(int matrixId, PartitionKey part, int count, long[] nodeIds, int[] types
			, int startIndex, int endIndex) {
		super(matrixId, part);
		this.nodeIds = nodeIds;
		this.types = types;
		this.count = count;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
	}

	public PartSampleNeighborParam() {
		this(0, null, 0, null, null, 0, 0);
	}

	public long[] getNodeIds() {
		return nodeIds;
	}

	public void setNodeIds(long[] nodeIds) {
		this.nodeIds = nodeIds;
	}

	public int[] getTypes() {
		return types;
	}

	public void setTypes(int[] types) {
		this.types = types;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public int getStartIndex() {
		return startIndex;
	}

	public int getEndIndex() {
		return endIndex;
	}

	@Override
	public void serialize(ByteBuf buf) {
		super.serialize(buf);
		buf.writeInt(count);
		buf.writeInt(endIndex - startIndex);
		for (int i = startIndex; i < endIndex; i++) {
			buf.writeLong(nodeIds[i]);
		}

		if (types == null) {
			buf.writeInt(0);
		} else {
			buf.writeInt(types.length);
			for (int i = 0; i < types.length; i++) {
				buf.writeInt(types[i]);
			}
		}
	}

	@Override
	public void deserialize(ByteBuf buf) {
		super.deserialize(buf);
		count = buf.readInt();
		nodeIds = new long[buf.readInt()];
		for (int i = 0; i < nodeIds.length; i++) {
			nodeIds[i] = buf.readLong();
		}

		int len = buf.readInt();
		if (0 != len) {
			types = new int[len];
			for (int i = 0; i < len; i++) {
				types[i] = buf.readInt();
			}
		}
	}

	@Override
	public int bufferLen() {
		int len = super.bufferLen() + 4 + 8 * (endIndex - startIndex) + 4;
		if (null != types) {
			len += 4 * types.length;
		}
		return len;
	}
}
