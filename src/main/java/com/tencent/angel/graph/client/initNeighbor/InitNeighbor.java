package com.tencent.angel.graph.client.initNeighbor;

import com.tencent.angel.graph.data.Node;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;

public class InitNeighbor extends UpdateFunc {

	public InitNeighbor(InitNeighborParam param) {
		super(param);
	}

	public InitNeighbor() {
		this(null);
	}

	@Override
	public void partitionUpdate(PartitionUpdateParam partParam) {
		InitNeighborPartParam param = (InitNeighborPartParam) partParam;
		ServerLongAnyRow row = (ServerLongAnyRow) (psContext.getMatrixStorageManager().getRow(param.getPartKey(), 0));

		long[] keys = param.getKeys();
		long[][] neighborArrays = param.getNeighborArrays();
		int[][] neighborTypes = param.getTypeArrays();
		float[][] neighborWeights = param.getWeightArrays();

		row.startWrite();
		try {
			for (int i = 0; i < keys.length; i++) {
				Node node = (Node) row.get(keys[i]);
				if (node == null) {
					node = new Node();
					row.set(keys[i], node);
				}

				node.setNeighbors(neighborArrays[i]);
				if (neighborTypes != null)
					node.setTypes(neighborTypes[i]);
				if (neighborWeights != null)
					node.setWeights(neighborWeights[i]);
			}
		} finally {
			row.endWrite();
		}

	}

}