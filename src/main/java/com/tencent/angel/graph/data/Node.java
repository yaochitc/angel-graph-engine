package com.tencent.angel.graph.data;

import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Node features
 */
public class Node implements IElement {

    private IntFloatVector feats;

    private long[] neighbors;
    private int[] types;
    private float[] weights;
    private float[] typeAccSumWeights;
    private float[] nodeAccSumWeights;

    public Node(IntFloatVector feats, long[] neighbors, int[] types, float[] weights, float[] typeAccSumWeights, float[] nodeAccSumWeights) {
        this.feats = feats;
        this.neighbors = neighbors;
        this.types = types;
        this.weights = weights;
        this.typeAccSumWeights = typeAccSumWeights;
        this.nodeAccSumWeights = nodeAccSumWeights;
    }

    public Node() {
        this(null, null, null, null, null, null);
    }

    public IntFloatVector getFeats() {
        return feats;
    }

    public void setFeats(IntFloatVector feats) {
        this.feats = feats;
    }

    public long[] getNeighbors() {
        return neighbors;
    }

    public void setNeighbors(long[] neighbors) {
        this.neighbors = neighbors;
    }

    public int[] getTypes() {
        return types;
    }

    public void setTypes(int[] types) {
        this.types = types;
    }

    public float[] getWeights() {
        return weights;
    }

    public void setWeights(float[] weights) {
        this.weights = weights;
    }

    @Override
    public Node deepClone() {
        IntFloatVector cloneFeats = feats.clone();

        long[] cloneNeighbors = new long[neighbors.length];
        System.arraycopy(neighbors, 0, cloneNeighbors, 0, neighbors.length);

        int[] cloneTypes = null;
        if (types != null) {
            cloneTypes = new int[types.length];
            System.arraycopy(types, 0, cloneTypes, 0, types.length);
        }

        float[] cloneWeights = null;
        if (weights != null) {
            cloneWeights = new float[weights.length];
            System.arraycopy(weights, 0, cloneWeights, 0, weights.length);
        }

        float[] cloneTypeAccSumWeights = null;
        if (typeAccSumWeights != null) {
            cloneTypeAccSumWeights = new float[typeAccSumWeights.length];
            System.arraycopy(typeAccSumWeights, 0, cloneTypeAccSumWeights, 0, typeAccSumWeights.length);
        }

        float[] cloneNodeAccSumWeights = null;
        if (nodeAccSumWeights != null) {
            cloneNodeAccSumWeights = new float[nodeAccSumWeights.length];
            System.arraycopy(nodeAccSumWeights, 0, cloneNodeAccSumWeights, 0, nodeAccSumWeights.length);
        }
        return new Node(cloneFeats, cloneNeighbors, cloneTypes, cloneWeights, cloneTypeAccSumWeights, cloneNodeAccSumWeights);
    }

    @Override
    public void serialize(ByteBuf output) {
        NodeUtils.serialize(feats, output);

        output.writeInt(neighbors.length);
        for (int i = 0; i < neighbors.length; i++)
            output.writeLong(neighbors[i]);

        if (types == null) {
            output.writeInt(0);
        } else {
            output.writeInt(types.length);
            for (int i = 0; i < types.length; i++)
                output.writeInt(types[i]);
        }

        if (weights == null) {
            output.writeInt(0);
        } else {
            output.writeInt(weights.length);
            for (int i = 0; i < weights.length; i++)
                output.writeFloat(weights[i]);
        }

        if (typeAccSumWeights == null) {
            output.writeInt(0);
        } else {
            output.writeInt(typeAccSumWeights.length);
            for (int i = 0; i < typeAccSumWeights.length; i++)
                output.writeFloat(typeAccSumWeights[i]);
        }

        if (nodeAccSumWeights == null) {
            output.writeInt(0);
        } else {
            output.writeInt(nodeAccSumWeights.length);
            for (int i = 0; i < nodeAccSumWeights.length; i++)
                output.writeFloat(nodeAccSumWeights[i]);
        }
    }

    @Override
    public void deserialize(ByteBuf input) {
        feats = NodeUtils.deserialize(input);

        int len = input.readInt();
        neighbors = new long[len];
        for (int i = 0; i < len; i++)
            neighbors[i] = input.readLong();

        len = input.readInt();
        if (len > 0) {
            types = new int[len];
            for (int i = 0; i < len; i++)
                types[i] = input.readInt();
        }

        len = input.readInt();
        if (len > 0) {
            weights = new float[len];
            for (int i = 0; i < len; i++)
                weights[i] = input.readFloat();
        }

        len = input.readInt();
        if (len > 0) {
            typeAccSumWeights = new float[len];
            for (int i = 0; i < len; i++)
                typeAccSumWeights[i] = input.readInt();
        }

        len = input.readInt();
        if (len > 0) {
            nodeAccSumWeights = new float[len];
            for (int i = 0; i < len; i++)
                nodeAccSumWeights[i] = input.readFloat();
        }
    }

    @Override
    public int bufferLen() {
        int len = NodeUtils.dataLen(feats);
        len += 4 + 8 * neighbors.length + 4 + 4 + 4 + 4;
        if (types != null)
            len += 4 * types.length;
        if (weights != null)
            len += 4 * weights.length;
        if (typeAccSumWeights != null)
            len += 4 * typeAccSumWeights.length;
        if (nodeAccSumWeights != null)
            len += 4 * nodeAccSumWeights.length;
        return len;
    }

    @Override
    public void serialize(DataOutputStream output) throws IOException {
        NodeUtils.serialize(feats, output);

        output.writeInt(neighbors.length);
        for (int i = 0; i < neighbors.length; i++)
            output.writeLong(neighbors[i]);

        if (types == null) {
            output.writeInt(0);
        } else {
            output.writeInt(types.length);
            for (int i = 0; i < types.length; i++)
                output.writeInt(types[i]);
        }

        if (weights == null) {
            output.writeInt(0);
        } else {
            output.writeInt(weights.length);
            for (int i = 0; i < weights.length; i++)
                output.writeFloat(weights[i]);
        }

        if (typeAccSumWeights == null) {
            output.writeInt(0);
        } else {
            output.writeInt(typeAccSumWeights.length);
            for (int i = 0; i < typeAccSumWeights.length; i++)
                output.writeFloat(typeAccSumWeights[i]);
        }

        if (nodeAccSumWeights == null) {
            output.writeInt(0);
        } else {
            output.writeInt(nodeAccSumWeights.length);
            for (int i = 0; i < nodeAccSumWeights.length; i++)
                output.writeFloat(nodeAccSumWeights[i]);
        }
    }

    @Override
    public void deserialize(DataInputStream input) throws IOException {
        feats = NodeUtils.deserialize(input);

        int len = input.readInt();
        neighbors = new long[len];
        for (int i = 0; i < len; i++)
            neighbors[i] = input.readLong();

        len = input.readInt();
        if (len > 0) {
            types = new int[len];
            for (int i = 0; i < len; i++)
                types[i] = input.readInt();
        }

        len = input.readInt();
        if (len > 0) {
            weights = new float[len];
            for (int i = 0; i < len; i++)
                weights[i] = input.readFloat();
        }

        len = input.readInt();
        if (len > 0) {
            typeAccSumWeights = new float[len];
            for (int i = 0; i < len; i++)
                typeAccSumWeights[i] = input.readInt();
        }

        len = input.readInt();
        if (len > 0) {
            nodeAccSumWeights = new float[len];
            for (int i = 0; i < len; i++)
                nodeAccSumWeights[i] = input.readFloat();
        }
    }

    @Override
    public int dataLen() {
        return bufferLen();
    }
}
