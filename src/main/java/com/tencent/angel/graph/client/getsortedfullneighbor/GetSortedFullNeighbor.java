package com.tencent.angel.graph.client.getsortedfullneighbor;

import com.tencent.angel.graph.data.Neighbor;
import com.tencent.angel.graph.data.Node;
import com.tencent.angel.graph.util.LongIndexComparator;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.List;

public class GetSortedFullNeighbor extends GetFunc {

	public GetSortedFullNeighbor(GetSortedFullNeighborParam param) {
		super(param);
	}

	public GetSortedFullNeighbor() {
		this(null);
	}

	@Override
	public PartitionGetResult partitionGet(PartitionGetParam partParam) {
		PartGetSortedFullNeighborParam param = (PartGetSortedFullNeighborParam) partParam;
		ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
		ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
		ServerLongAnyRow row = (ServerLongAnyRow) (((RowBasedPartition) part).getRow(0));
		long[] nodeIds = param.getNodeIds();
		int[] types = param.getTypes();
		Neighbor[] neighbors = new Neighbor[nodeIds.length];

		for (int i = 0; i < nodeIds.length; i++) {
			long nodeId = nodeIds[i];

			Node element = (Node) (row.get(nodeId));
			if (element == null) {
				neighbors[i] = null;
				continue;
			}

			long[] nodeNeighbors = element.getNeighbors();
			int[] nodeTypes = element.getTypes();
			float[] nodeWeights = element.getWeights();

			if (nodeNeighbors == null || nodeNeighbors.length == 0) {
				neighbors[i] = null;
				continue;
			}

			if (types == null || types.length == 0) {
				neighbors[i] = buildNeighbor(nodeNeighbors, nodeWeights);
				continue;
			}

			IntSet typeSet = new IntRBTreeSet(types);

			LongArrayList nodeNeighborList = new LongArrayList();
			FloatArrayList nodeWeightList = new FloatArrayList();

			for (int j = 0; j < nodeNeighbors.length; j++) {
				if (typeSet.contains(nodeTypes[j])) {
					nodeNeighborList.add(nodeNeighbors[j]);
					nodeWeightList.add(nodeWeights[j]);
				}
			}

			neighbors[i] = buildNeighbor(nodeNeighborList.toLongArray(), nodeWeightList.toFloatArray());
		}

		return new PartGetSortedFullNeighborResult(part.getPartitionKey().getPartitionId(), neighbors);
	}

	private static Neighbor buildNeighbor(long[] nodeNeighbors, float[] nodeWeights) {
		int size = nodeNeighbors.length;
		int[] index = new int[size];
		for (int i = 0; i < size; i++)
			index[i] = i;

		LongIndexComparator comparator = new LongIndexComparator(nodeNeighbors);
		IntArrays.quickSort(index, comparator);

		long[] sortedNodeNeighbors = new long[size];
		float[] sortedNodeWeights = new float[size];

		for (int i = 0; i < size; i++) {
			sortedNodeNeighbors[i] = nodeNeighbors[index[i]];
			sortedNodeWeights[i] = nodeWeights[index[i]];
		}
		return new Neighbor(sortedNodeNeighbors, sortedNodeWeights);
	}

	@Override
	public GetResult merge(List<PartitionGetResult> partResults) {
		Int2ObjectArrayMap<PartitionGetResult> partIdToResultMap = new Int2ObjectArrayMap<>(
				partResults.size());
		for (PartitionGetResult result : partResults) {
			partIdToResultMap.put(((PartGetSortedFullNeighborResult) result).getPartId(), result);
		}

		GetSortedFullNeighborParam param = (GetSortedFullNeighborParam) getParam();
		long[] nodeIds = param.getNodeIds();
		List<PartitionGetParam> partParams = param.getPartParams();

		Long2ObjectOpenHashMap<Neighbor> nodeIdToNeighbors = new Long2ObjectOpenHashMap<>(nodeIds.length);

		for (PartitionGetParam partParam : partParams) {
			int start = ((PartGetSortedFullNeighborParam) partParam).getStartIndex();
			int end = ((PartGetSortedFullNeighborParam) partParam).getEndIndex();
			PartGetSortedFullNeighborResult partResult = (PartGetSortedFullNeighborResult) (partIdToResultMap
					.get(partParam.getPartKey().getPartitionId()));
			Neighbor[] results = partResult.getNodeIdToNeighbors();
			for (int i = start; i < end; i++) {
				nodeIdToNeighbors.put(nodeIds[i], results[i - start]);
			}
		}

		return new GetSortedFullNeighborResult(nodeIdToNeighbors);
	}
}
