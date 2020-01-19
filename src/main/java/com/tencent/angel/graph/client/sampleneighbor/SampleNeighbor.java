package com.tencent.angel.graph.client.sampleneighbor;

import com.tencent.angel.ml.matrix.psf.get.base.*;

import java.util.List;

public class SampleNeighbor extends GetFunc {

	public SampleNeighbor(SampleNeighborParam param) {
		super(param);
	}

	public SampleNeighbor() {
		this(null);
	}

	@Override
	public PartitionGetResult partitionGet(PartitionGetParam partitionGetParam) {
		return null;
	}

	@Override
	public GetResult merge(List<PartitionGetResult> list) {
		return null;
	}
}
