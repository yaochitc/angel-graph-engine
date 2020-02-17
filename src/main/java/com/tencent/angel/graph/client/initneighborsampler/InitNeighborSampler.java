package com.tencent.angel.graph.client.initneighborsampler;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;

public class InitNeighborSampler extends UpdateFunc {

	public InitNeighborSampler(InitNeighborSamplerParam param) {
		super(param);
	}

	public InitNeighborSampler() {
		this(null);
	}

	@Override
	public void partitionUpdate(PartitionUpdateParam partParam) {
		InitNeighborSamplerPartParam param = (InitNeighborSamplerPartParam) partParam;
		ServerLongAnyRow row = (ServerLongAnyRow) (psContext.getMatrixStorageManager().getRow(param.getPartKey(), 0));

		row.startWrite();
		try {

		} finally {
			row.endWrite();
		}

	}

}