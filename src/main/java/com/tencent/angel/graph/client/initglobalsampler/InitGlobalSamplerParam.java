package com.tencent.angel.graph.client.initglobalsampler;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.graph.client.initneighbor.InitNeighborPartParam;
import com.tencent.angel.graph.util.LongIndexComparator;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;
import it.unimi.dsi.fastutil.ints.IntArrays;

import java.util.ArrayList;
import java.util.List;

public class InitGlobalSamplerParam extends UpdateParam {

    private long[] nodeIds;
    private int[] types;
    private float[] weights;
    private int start;
    private int end;

    public InitGlobalSamplerParam(int matrixId, long[] nodeIds,
                             int start, int end) {
        this(matrixId, nodeIds, null, null, start, end);
    }

    public InitGlobalSamplerParam(int matrixId, long[] nodeIds,
                             int[] types, float[] weights,
                             int start, int end) {
        super(matrixId);
        this.nodeIds = nodeIds;
        this.types = types;
        this.weights = weights;
        this.start = start;
        this.end = end;
    }

    @Override
    public List<PartitionUpdateParam> split() {
        LongIndexComparator comparator = new LongIndexComparator(nodeIds);
        int size = end - start;
        int[] index = new int[size];
        for (int i = 0; i < size; i++)
            index[i] = i + start;
        IntArrays.quickSort(index, comparator);

        List<PartitionUpdateParam> params = new ArrayList<>();
        List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

        if (!RowUpdateSplitUtils.isInRange(nodeIds, index, parts)) {
            throw new AngelException(
                    "node id is not in range [" + parts.get(0).getStartCol() + ", " + parts
                            .get(parts.size() - 1).getEndCol());
        }

        int nodeIndex = start;
        int partIndex = 0;
        while (nodeIndex < end || partIndex < parts.size()) {
            int length = 0;
            long endOffset = parts.get(partIndex).getEndCol();
            while (nodeIndex < end && nodeIds[index[nodeIndex - start]] < endOffset) {
                nodeIndex++;
                length++;
            }

            if (length > 0)
                params.add(new InitGlobalSamplerPartParam(matrixId,
                        parts.get(partIndex), index, nodeIds, types, weights,
                        nodeIndex - length - start, nodeIndex - start));

            partIndex++;
        }

        return params;
    }
}
