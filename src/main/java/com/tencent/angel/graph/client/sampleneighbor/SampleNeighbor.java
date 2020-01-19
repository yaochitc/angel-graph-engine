package com.tencent.angel.graph.client.sampleneighbor;

import com.tencent.angel.graph.data.Neighbor;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.List;

public class SampleNeighbor extends GetFunc {

	public SampleNeighbor(SampleNeighborParam param) {
		super(param);
	}

	public SampleNeighbor() {
		this(null);
	}

	@Override
	public PartitionGetResult partitionGet(PartitionGetParam partParam) {
		PartSampleNeighborParam param = (PartSampleNeighborParam) partParam;
		ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
		ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
		ServerLongAnyRow row = (ServerLongAnyRow) (((RowBasedPartition) part).getRow(0));
		long[] nodeIds = param.getNodeIds();
		Neighbor[] neighbors = new Neighbor[nodeIds.length];

		int count = param.getCount();

		return new PartSampleNeighborResult(part.getPartitionKey().getPartitionId(), neighbors);
	}

	@Override
	public GetResult merge(List<PartitionGetResult> partResults) {
		Int2ObjectArrayMap<PartitionGetResult> partIdToResultMap = new Int2ObjectArrayMap<>(
				partResults.size());
		for (PartitionGetResult result : partResults) {
			partIdToResultMap.put(((PartSampleNeighborResult) result).getPartId(), result);
		}

		SampleNeighborParam param = (SampleNeighborParam) getParam();
		long[] nodeIds = param.getNodeIds();
		List<PartitionGetParam> partParams = param.getPartParams();

		Long2ObjectOpenHashMap<Neighbor> nodeIdToNeighbors = new Long2ObjectOpenHashMap<>(nodeIds.length);

		for (PartitionGetParam partParam : partParams) {
			int start = ((PartSampleNeighborParam) partParam).getStartIndex();
			int end = ((PartSampleNeighborParam) partParam).getEndIndex();
			PartSampleNeighborResult partResult = (PartSampleNeighborResult) (partIdToResultMap
					.get(partParam.getPartKey().getPartitionId()));
			Neighbor[] results = partResult.getNodeIdToNeighbors();
			for (int i = start; i < end; i++) {
				nodeIdToNeighbors.put(nodeIds[i], results[i - start]);
			}
		}

		return new SampleNeighborResult(nodeIdToNeighbors);
	}
}
