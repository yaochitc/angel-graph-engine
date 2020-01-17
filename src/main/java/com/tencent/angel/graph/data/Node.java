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

	public Node(IntFloatVector feats, long[] neighbors) {
		this(feats, neighbors, null, null);
	}

	public Node(IntFloatVector feats, long[] neighbors, int[] types, float[] weights) {
		this.feats = feats;
		this.neighbors = neighbors;
		this.types = types;
		this.weights = weights;
	}

	public Node() {
		this(null, null, null, null);
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
		return new Node(cloneFeats, cloneNeighbors, cloneTypes, cloneWeights);
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
	}

	@Override
	public void deserialize(ByteBuf input) {
		feats = NodeUtils.deserialize(input);

		int len = input.readInt();
		neighbors = new long[len];
		for (int i = 0; i < len; i++)
			neighbors[i] = input.readLong();

		len = input.readInt();
		types = new int[len];
		for (int i = 0; i < len; i++)
			types[i] = input.readInt();

		len = input.readInt();
		weights = new float[len];
		for (int i = 0; i < len; i++)
			weights[i] = input.readFloat();
	}

	@Override
	public int bufferLen() {
		int len = NodeUtils.dataLen(feats);
		len += 4 + 8 * neighbors.length + 4 + 4;
		if (types != null)
			len += 4 * types.length;
		if (weights != null)
			len += 4 * weights.length;
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
	}

	@Override
	public void deserialize(DataInputStream input) throws IOException {
		feats = NodeUtils.deserialize(input);

		int len = input.readInt();
		neighbors = new long[len];
		for (int i = 0; i < len; i++)
			neighbors[i] = input.readLong();

		len = input.readInt();
		types = new int[len];
		for (int i = 0; i < len; i++)
			types[i] = input.readInt();

		len = input.readInt();
		weights = new float[len];
		for (int i = 0; i < len; i++)
			weights[i] = input.readFloat();
	}

	@Override
	public int dataLen() {
		return bufferLen();
	}
}
