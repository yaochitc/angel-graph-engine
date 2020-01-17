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

	private float weight;
	private long[] neighbors;
	private int[] edgeTypes;

	public Node(IntFloatVector feats, long[] neighbors) {
		this(1.0f, feats, neighbors, null);
	}

	public Node(float weight, IntFloatVector feats, long[] neighbors, int[] edgeTypes) {
		this.weight = weight;
		this.feats = feats;
		this.neighbors = neighbors;
		this.edgeTypes = edgeTypes;
	}

	public Node() {
		this(0f, null, null, null);
	}

	public IntFloatVector getFeats() {
		return feats;
	}

	public void setFeats(IntFloatVector feats) {
		this.feats = feats;
	}

	public float getWeight() {
		return weight;
	}

	public void setWeight(float weight) {
		this.weight = weight;
	}

	public long[] getNeighbors() {
		return neighbors;
	}

	public void setNeighbors(long[] neighbors) {
		this.neighbors = neighbors;
	}

	public int[] getEdgeTypes() {
		return edgeTypes;
	}

	public void setEdgeTypes(int[] edgeTypes) {
		this.edgeTypes = edgeTypes;
	}

	@Override
	public Node deepClone() {
		IntFloatVector cloneFeats = feats.clone();

		long[] cloneNeighbors = new long[neighbors.length];
		System.arraycopy(neighbors, 0, cloneNeighbors, 0, neighbors.length);

		if (edgeTypes == null)
			return new Node(weight, cloneFeats, cloneNeighbors, null);
		else {
			int[] cloneEdgeTypes = new int[edgeTypes.length];
			System.arraycopy(edgeTypes, 0, cloneEdgeTypes, 0, edgeTypes.length);
			return new Node(weight, cloneFeats, cloneNeighbors, cloneEdgeTypes);
		}
	}

	@Override
	public void serialize(ByteBuf output) {
		NodeUtils.serialize(feats, output);

		output.writeFloat(weight);

		output.writeInt(neighbors.length);
		for (int i = 0; i < neighbors.length; i++)
			output.writeLong(neighbors[i]);

		if (edgeTypes != null) {
			output.writeInt(edgeTypes.length);
			for (int i = 0; i < edgeTypes.length; i++)
				output.writeInt(edgeTypes[i]);
		}
	}

	@Override
	public void deserialize(ByteBuf input) {
		feats = NodeUtils.deserialize(input);

		weight = input.readFloat();

		int len = input.readInt();
		neighbors = new long[len];
		for (int i = 0; i < len; i++)
			neighbors[i] = input.readLong();

		if (edgeTypes != null) {
			len = input.readInt();
			edgeTypes = new int[len];
			for (int i = 0; i < len; i++)
				edgeTypes[i] = input.readInt();
		}
	}

	@Override
	public int bufferLen() {
		int len = NodeUtils.dataLen(feats);
		len += 4;
		len += 4 + 8 * neighbors.length;
		if (edgeTypes != null)
			len += 4 + 4 * edgeTypes.length;
		return len;
	}

	@Override
	public void serialize(DataOutputStream output) throws IOException {
		NodeUtils.serialize(feats, output);

		output.writeFloat(weight);

		output.writeInt(neighbors.length);
		for (int i = 0; i < neighbors.length; i++)
			output.writeLong(neighbors[i]);

		if (edgeTypes != null) {
			output.writeInt(edgeTypes.length);
			for (int i = 0; i < edgeTypes.length; i++)
				output.writeInt(edgeTypes[i]);
		}
	}

	@Override
	public void deserialize(DataInputStream input) throws IOException {
		feats = NodeUtils.deserialize(input);

		weight = input.readFloat();

		int len = input.readInt();
		neighbors = new long[len];
		for (int i = 0; i < len; i++)
			neighbors[i] = input.readLong();

		if (edgeTypes != null) {
			len = input.readInt();
			edgeTypes = new int[len];
			for (int i = 0; i < len; i++)
				edgeTypes[i] = input.readInt();
		}
	}

	@Override
	public int dataLen() {
		return bufferLen();
	}
}
