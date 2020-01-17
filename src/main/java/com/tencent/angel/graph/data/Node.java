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
	private int[] edgeTypes;
	private float[] edgeWeights;

	public Node(IntFloatVector feats, long[] neighbors) {
		this(feats, neighbors, null, null);
	}

	public Node(IntFloatVector feats, long[] neighbors, int[] edgeTypes, float[] edgeWeights) {
		this.feats = feats;
		this.neighbors = neighbors;
		this.edgeTypes = edgeTypes;
		this.edgeWeights = edgeWeights;
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

	public int[] getEdgeTypes() {
		return edgeTypes;
	}

	public void setEdgeTypes(int[] edgeTypes) {
		this.edgeTypes = edgeTypes;
	}

	public float[] getEdgeWeights() {
		return edgeWeights;
	}

	public void setEdgeWeights(float[] edgeWeights) {
		this.edgeWeights = edgeWeights;
	}

	@Override
	public Node deepClone() {
		IntFloatVector cloneFeats = feats.clone();

		long[] cloneNeighbors = new long[neighbors.length];
		System.arraycopy(neighbors, 0, cloneNeighbors, 0, neighbors.length);

		int[] cloneEdgeTypes = null;
		if (edgeTypes != null) {
			cloneEdgeTypes = new int[edgeTypes.length];
			System.arraycopy(edgeTypes, 0, cloneEdgeTypes, 0, edgeTypes.length);
		}

		float[] cloneEdgeWeights = null;
		if (edgeWeights != null) {
			cloneEdgeWeights = new float[edgeWeights.length];
			System.arraycopy(edgeWeights, 0, cloneEdgeWeights, 0, edgeWeights.length);
		}
		return new Node(cloneFeats, cloneNeighbors, cloneEdgeTypes, cloneEdgeWeights);
	}

	@Override
	public void serialize(ByteBuf output) {
		NodeUtils.serialize(feats, output);

		output.writeInt(neighbors.length);
		for (int i = 0; i < neighbors.length; i++)
			output.writeLong(neighbors[i]);

		if (edgeTypes != null) {
			output.writeInt(edgeTypes.length);
			for (int i = 0; i < edgeTypes.length; i++)
				output.writeInt(edgeTypes[i]);
		}

		if (edgeWeights != null) {
			output.writeInt(edgeWeights.length);
			for (int i = 0; i < edgeWeights.length; i++)
				output.writeFloat(edgeWeights[i]);
		}
	}

	@Override
	public void deserialize(ByteBuf input) {
		feats = NodeUtils.deserialize(input);

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

		if (edgeWeights != null) {
			len = input.readInt();
			edgeWeights = new float[len];
			for (int i = 0; i < len; i++)
				edgeWeights[i] = input.readFloat();
		}
	}

	@Override
	public int bufferLen() {
		int len = NodeUtils.dataLen(feats);
		len += 4 + 8 * neighbors.length;
		if (edgeTypes != null)
			len += 4 + 4 * edgeTypes.length;
		if (edgeWeights != null)
			len += 4 + 4 * edgeWeights.length;
		return len;
	}

	@Override
	public void serialize(DataOutputStream output) throws IOException {
		NodeUtils.serialize(feats, output);

		output.writeInt(neighbors.length);
		for (int i = 0; i < neighbors.length; i++)
			output.writeLong(neighbors[i]);

		if (edgeTypes != null) {
			output.writeInt(edgeTypes.length);
			for (int i = 0; i < edgeTypes.length; i++)
				output.writeInt(edgeTypes[i]);
		}

		if (edgeWeights != null) {
			output.writeInt(edgeWeights.length);
			for (int i = 0; i < edgeWeights.length; i++)
				output.writeFloat(edgeWeights[i]);
		}
	}

	@Override
	public void deserialize(DataInputStream input) throws IOException {
		feats = NodeUtils.deserialize(input);

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

		if (edgeWeights != null) {
			len = input.readInt();
			edgeWeights = new float[len];
			for (int i = 0; i < len; i++)
				edgeWeights[i] = input.readFloat();
		}
	}

	@Override
	public int dataLen() {
		return bufferLen();
	}
}
