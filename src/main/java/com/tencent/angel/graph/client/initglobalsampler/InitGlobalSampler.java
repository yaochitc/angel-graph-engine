package com.tencent.angel.graph.client.initglobalsampler;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;

public class InitGlobalSampler extends UpdateFunc {

    public InitGlobalSampler(InitGlobalSamplerParam param) {
        super(param);
    }

    public InitGlobalSampler() {
        this(null);
    }

    @Override
    public void partitionUpdate(PartitionUpdateParam partParam) {
        InitGlobalSamplerPartParam param = (InitGlobalSamplerPartParam) partParam;
        ServerLongAnyRow row = (ServerLongAnyRow) (psContext.getMatrixStorageManager().getRow(param.getPartKey(), 0));

        row.startWrite();
        try {

        } finally {
            row.endWrite();
        }

    }

}