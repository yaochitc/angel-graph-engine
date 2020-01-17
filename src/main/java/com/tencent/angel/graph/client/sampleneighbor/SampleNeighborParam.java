package com.tencent.angel.graph.client.sampleneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SampleNeighborParam extends GetParam {

	private final long[] nodeIds;

	private int[] types;

	private final int count;

	private final List<PartitionGetParam> partParams;

	public SampleNeighborParam(int matrixId, long[] nodeIds, int[] types, int count) {
		super(matrixId);
		this.nodeIds = nodeIds;
		this.types = types;
		this.count = count;
		this.partParams = new ArrayList<>();
	}

	public SampleNeighborParam() {
		this(-1, null, null, -1);
	}

	public long[] getNodeIds() {
		return nodeIds;
	}

	public int[] getTypes() {
		return types;
	}

	public int getCount() {
		return count;
	}

	public List<PartitionGetParam> getPartParams() {
		return partParams;
	}

	@Override
	public List<PartitionGetParam> split() {
		Arrays.sort(nodeIds);

		List<PartitionKey> partitions =
				PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

		if (!RowUpdateSplitUtils.isInRange(nodeIds, partitions)) {
			throw new AngelException(
					"node id is not in range [" + partitions.get(0).getStartCol() + ", " + partitions
							.get(partitions.size() - 1).getEndCol());
		}

		int nodeIndex = 0;
		int partIndex = 0;
		while (nodeIndex < nodeIds.length || partIndex < partitions.size()) {
			int length = 0;
			long endOffset = partitions.get(partIndex).getEndCol();
			while (nodeIndex < nodeIds.length && nodeIds[nodeIndex] < endOffset) {
				nodeIndex++;
				length++;
			}

			if (length > 0) {
				partParams.add(new PartSampleNeighborParam(matrixId,
						partitions.get(partIndex), count, nodeIds, types, nodeIndex - length, nodeIndex));
			}
			partIndex++;
		}

		return partParams;
	}
}
