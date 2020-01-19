package com.tencent.angel.graph.client.getfullneighbor;

import com.tencent.angel.graph.data.Neighbor;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.List;

public class GetFullNeighbor extends GetFunc {

	public GetFullNeighbor(GetFullNeighborParam param) {
		super(param);
	}

	public GetFullNeighbor() {
		this(null);
	}

	@Override
	public PartitionGetResult partitionGet(PartitionGetParam partParam) {
		PartGetFullNeighborParam param = (PartGetFullNeighborParam) partParam;
		ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
		ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
		ServerLongAnyRow row = (ServerLongAnyRow) (((RowBasedPartition) part).getRow(0));
		long[] nodeIds = param.getNodeIds();
		Neighbor[] neighbors = new Neighbor[nodeIds.length];

		return new PartGetFullNeighborResult(part.getPartitionKey().getPartitionId(), neighbors);
	}

	@Override
	public GetResult merge(List<PartitionGetResult> partResults) {
		Int2ObjectArrayMap<PartitionGetResult> partIdToResultMap = new Int2ObjectArrayMap<>(
				partResults.size());
		for (PartitionGetResult result : partResults) {
			partIdToResultMap.put(((PartGetFullNeighborResult) result).getPartId(), result);
		}

		GetFullNeighborParam param = (GetFullNeighborParam) getParam();
		long[] nodeIds = param.getNodeIds();
		List<PartitionGetParam> partParams = param.getPartParams();

		Long2ObjectOpenHashMap<Neighbor> nodeIdToNeighbors = new Long2ObjectOpenHashMap<>(nodeIds.length);

		for (PartitionGetParam partParam : partParams) {
			int start = ((PartGetFullNeighborParam) partParam).getStartIndex();
			int end = ((PartGetFullNeighborParam) partParam).getEndIndex();
			PartGetFullNeighborResult partResult = (PartGetFullNeighborResult) (partIdToResultMap
					.get(partParam.getPartKey().getPartitionId()));
			Neighbor[] results = partResult.getNodeIdToNeighbors();
			for (int i = start; i < end; i++) {
				nodeIdToNeighbors.put(nodeIds[i], results[i - start]);
			}
		}

		return new GetFullNeighborResult(nodeIdToNeighbors);
	}
}
