package com.tencent.angel.graph.client.initnodesampler;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;

public class InitNodeSampler extends UpdateFunc {

    public InitNodeSampler(InitNodeSamplerParam param) {
        super(param);
    }

    public InitNodeSampler() {
        this(null);
    }

    @Override
    public void partitionUpdate(PartitionUpdateParam partParam) {
        InitNodeSamplerPartParam param = (InitNodeSamplerPartParam) partParam;
        ServerLongAnyRow row = (ServerLongAnyRow) (psContext.getMatrixStorageManager().getRow(param.getPartKey(), 0));

        row.startWrite();
        try {

        } finally {
            row.endWrite();
        }

    }

}