package com.tencent.angel.graph.client.samplenode;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;

public class SampleNodeParam extends GetParam {

	private int[][] typeGroupIndexArray;

	private final int[][] countArray;

	private final List<PartitionGetParam> partParams;

	public SampleNodeParam(int matrixId, int[][] typeGroupIndexArray, int[][] countArray) {
		super(matrixId);
		this.typeGroupIndexArray = typeGroupIndexArray;
		this.countArray = countArray;
		this.partParams = new ArrayList<>();
	}

	public SampleNodeParam() {
		this(-1, null, null);
	}

	public int[][] getTypeGroupIndexArray() {
		return typeGroupIndexArray;
	}

	public int[][] getCountArray() {
		return countArray;
	}

	public List<PartitionGetParam> getPartParams() {
		return partParams;
	}

	@Override
	public List<PartitionGetParam> split() {
		List<PartitionKey> partitions =
				PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

		int partIndex = 0;
		while (partIndex < partitions.size()) {
				partParams.add(new PartSampleNodeParam(matrixId,
						partitions.get(partIndex), countArray[partIndex], typeGroupIndexArray[partIndex]));
			partIndex++;
		}

		return partParams;
	}
}
