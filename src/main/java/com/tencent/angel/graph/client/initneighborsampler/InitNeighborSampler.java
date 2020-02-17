package com.tencent.angel.graph.client.initneighborsampler;

import com.tencent.angel.graph.data.Node;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap.Entry;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

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
            ObjectIterator<Entry<IElement>> nodeIterator = row.iterator();
            while (nodeIterator.hasNext()) {
                Node node = (Node) nodeIterator.next().getValue();
                long[] nodeNeighbors = node.getNeighbors();
                int[] types = node.getTypes();
                float[] weights = node.getWeights();

                if (nodeNeighbors == null || nodeNeighbors.length == 0) {
                    continue;
                }

                Int2ObjectMap<LongArrayList> type2Neighbors = new Int2ObjectRBTreeMap<>();
                Int2ObjectMap<FloatArrayList> type2Weights = new Int2ObjectRBTreeMap<>();

                for (int i = 0; i < types.length; i++) {
                    LongArrayList nodeNeighborList = type2Neighbors.get(types[i]);
                    if (null == nodeNeighborList) {
                        nodeNeighborList = new LongArrayList();
                        type2Neighbors.put(types[i], nodeNeighborList);
                    }
                    nodeNeighborList.add(nodeNeighbors[i]);

                    FloatArrayList nodeWeightList = type2Weights.get(types[i]);
                    if (null == nodeWeightList) {
                        nodeWeightList = new FloatArrayList();
                        type2Weights.put(types[i], nodeWeightList);
                    }
                    nodeWeightList.add(weights[i]);
                }

                float[] typeAccSumWeights = new float[type2Neighbors.size()];
                int[] typeGroupIndices = new int[type2Neighbors.size()];
                long[] typeNodeNeighbors = new long[nodeNeighbors.length];
                float[] nodeAccSumWeights = new float[nodeNeighbors.length];

                int typeIndex = 0;
                int neighborIndex = 0;
                float typeAccSumWeight = 0;
                for (int type : type2Neighbors.keySet()) {
                    LongArrayList nodeNeighborList = type2Neighbors.get(type);
                    FloatArrayList nodeWeightList = type2Weights.get(type);
                    int numNeighbor = nodeNeighborList.size();

                    float curTypeAccSumWeight = typeAccSumWeight;
                    float curNodeAccSumWeight = 0;
                    for (int i = 0; i < numNeighbor; i++) {
                        float weight = nodeWeightList.getFloat(i);
                        long neighbor = nodeNeighborList.getLong(i);
                        curTypeAccSumWeight += weight;
                        curNodeAccSumWeight += weight;
                        typeNodeNeighbors[neighborIndex] = neighbor;
                        nodeAccSumWeights[neighborIndex] = curNodeAccSumWeight;
                        neighborIndex++;
                    }

                    typeAccSumWeights[typeIndex] = curTypeAccSumWeight;
                    typeGroupIndices[typeIndex] = neighborIndex;
                    typeIndex++;
                }

                node.setNeighbors(typeNodeNeighbors);
                node.setTypeAccSumWeights(typeAccSumWeights);
                node.setTypeGroupIndices(typeGroupIndices);
                node.setNodeAccSumWeights(nodeAccSumWeights);
            }
        } finally {
            row.endWrite();
        }

    }

}