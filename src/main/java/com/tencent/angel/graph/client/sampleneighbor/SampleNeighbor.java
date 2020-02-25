package com.tencent.angel.graph.client.sampleneighbor;

import com.tencent.angel.graph.data.Neighbor;
import com.tencent.angel.graph.data.Node;
import com.tencent.angel.graph.util.CompactSampler;
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
        int[] types = param.getTypes();
        long[] nodeIds = param.getNodeIds();
        Neighbor[] neighbors = new Neighbor[nodeIds.length];
        int count = param.getCount();

        for (int i = 0; i < nodeIds.length; i++) {
            long nodeId = nodeIds[i];

            // Get node neighbor number
            Node node = (Node) (row.get(nodeId));
            if (node == null) {
                neighbors[i] = null;
                continue;
            }

            if (types == null || types.length == 0) {
                neighbors[i] = sampleHomoNeighbor(node, count);
            } else {
                neighbors[i] = sampleHeteroNeighbor(node, types, count);
            }
        }

        return new PartSampleNeighborResult(part.getPartitionKey().getPartitionId(), neighbors);
    }

    private Neighbor sampleHomoNeighbor(Node node, int count) {
        long[] nodeNeighbors = node.getNeighbors();
        if (nodeNeighbors == null || nodeNeighbors.length == 0) {
            return null;
        }

        // Neighbors
        long[] neighborNodeIds = new long[count];
        float[] neighborWeights = new float[count];

        for (int i = 0; i < count; i++) {
            // sample neighbor
            int startIndex = 0;
            int endIndex = nodeNeighbors.length - 1;
            int neighborNodeId = CompactSampler.randomSelect(node.getNodeAccSumWeights(), startIndex, endIndex - 1);

            neighborNodeIds[i] = node.getNeighbors()[neighborNodeId];
            neighborWeights[i] = node.getWeights()[neighborNodeId];
        }

        return new Neighbor(neighborNodeIds, neighborWeights);
    }

    private Neighbor sampleHeteroNeighbor(Node node, int[] types, int count) {
        int typeNum = types.length;
        long[] nodeNeighbors = node.getNeighbors();
        if (nodeNeighbors == null || nodeNeighbors.length == 0) {
            return null;
        }

        int[] subTypes = new int[typeNum];
        float[] subTypeAccSumWeights = new float[typeNum];
        float subTypeTotalSumWeights = 0;
        for (int i = 0; i < typeNum; i++) {
            int type = types[i];
            subTypes[i] = type;
            float edgeWeight = type == 0 ? node.getTypeAccSumWeights()[0] :
                    node.getTypeAccSumWeights()[type] - node.getTypeAccSumWeights()[type - 1];
            subTypeTotalSumWeights += edgeWeight;
            subTypeAccSumWeights[i] = subTypeTotalSumWeights;
        }

        // Neighbors
        long[] neighborNodeIds = new long[count];
        float[] neighborWeights = new float[count];

        for (int i = 0; i < count; i++) {
            // sample edge type
            int type = subTypes[CompactSampler.randomSelect(subTypeAccSumWeights, 0, typeNum - 1)];

            // sample neighbor
            int startIndex = type > 0 ? node.getTypeGroupIndices()[type - 1] : 0;
            int endIndex = node.getTypeGroupIndices()[type];
            int neighborNodeId = CompactSampler.randomSelect(node.getNodeAccSumWeights(), startIndex, endIndex - 1);

            neighborNodeIds[i] = node.getNeighbors()[neighborNodeId];
            neighborWeights[i] = node.getWeights()[neighborNodeId];
        }

        return new Neighbor(neighborNodeIds, neighborWeights);
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
