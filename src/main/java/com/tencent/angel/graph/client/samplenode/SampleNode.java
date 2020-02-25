package com.tencent.angel.graph.client.samplenode;

import com.tencent.angel.graph.client.sampleneighbor.SampleNeighborParam;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;

import java.util.List;

public class SampleNode extends GetFunc {

    public SampleNode(SampleNodeParam param) {
        super(param);
    }

    public SampleNode() {
        this(null);
    }

    @Override
    public PartitionGetResult partitionGet(PartitionGetParam partParam) {
        PartSampleNodeParam param = (PartSampleNodeParam) partParam;
        ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
        ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
        ServerLongAnyRow row = (ServerLongAnyRow) (((RowBasedPartition) part).getRow(0));
        int[] typeGroupIndices = param.getTypeGroupIndices();
        int[] counts = param.getCounts();

        int count = 0;
        for (int i=0;i<counts.length;i++) {
            count += counts[i];
        }

        long[] nodeIds = new long[count];

        return new PartSampleNodeResult(part.getPartitionKey().getPartitionId(), nodeIds);
    }

    @Override
    public GetResult merge(List<PartitionGetResult> partResults) {
        SampleNodeParam param = (SampleNodeParam) getParam();
        int[][] counts = param.getCountArray();

        int count = 0;
        for (int i=0;i<counts.length;i++) {
            for (int j =0;j< counts[i].length;j++) {
                count += counts[i][j];
            }
        }
        long[] nodeIds = new long[count];
        int nodeIndex = 0;
        for (PartitionGetResult partResult : partResults) {
            long[] results = ((PartSampleNodeResult)partResult).getNodeIds();
            for (int i = 0; i < results.length; i++) {
                nodeIds[nodeIndex] = results[i];
                nodeIndex++;
            }
        }

        return new SampleNodeResult(nodeIds);
    }
}
