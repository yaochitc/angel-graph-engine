package com.tencent.angel.graph.client.initneighborsampler;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;

public class InitNeighborSamplerParam extends UpdateParam {

    public InitNeighborSamplerParam(int matrixId) {
		super(matrixId);
    }

    @Override
    public List<PartitionUpdateParam> split() {
        List<PartitionUpdateParam> params = new ArrayList<>();
        List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

        int partIndex = 0;
        while (partIndex < parts.size()) {
            params.add(new InitNeighborSamplerPartParam(matrixId, parts.get(partIndex)));
        }

        return params;
    }
}
