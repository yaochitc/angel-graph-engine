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

import java.util.List;

public class GetFullNeighbor extends GetFunc {

	public GetFullNeighbor(GetFullNeighborParam param) {
		super(param);
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
	public GetResult merge(List<PartitionGetResult> list) {
		return null;
	}
}
