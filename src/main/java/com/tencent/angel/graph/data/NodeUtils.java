package com.tencent.angel.graph.data;

import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ps.storage.vector.storage.StorageMethod;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class NodeUtils {
	public static int dataLen(IntFloatVector feats) {
		return 12 + 4 * (int)feats.getSize();
	}

	public static void serialize(IntFloatVector feats, ByteBuf output) {
		output.writeInt(feats.getDim());
		output.writeInt((int) feats.getSize());
		if (feats.isDense()) {
			output.writeInt(StorageMethod.DENSE.getValue());
			float[] values = feats.getStorage().getValues();
			for (int i = 0; i < values.length; i++) {
				output.writeFloat(values[i]);
			}
		} else if (feats.isSparse()) {
			output.writeInt(StorageMethod.SPARSE.getValue());
			ObjectIterator<Int2FloatMap.Entry> iter = feats
					.getStorage().entryIterator();
			while (iter.hasNext()) {
				Int2FloatMap.Entry entry = iter.next();
				output.writeInt(entry.getIntKey());
				output.writeFloat(entry.getFloatValue());
			}
		} else if (feats.isSorted()) {
			output.writeInt(StorageMethod.SORTED.getValue());
			int[] keys = feats.getStorage().getIndices();
			float[] values = feats.getStorage().getValues();
			for (int i = 0; i < keys.length; i++) {
				output.writeInt(keys[i]);
				output.writeFloat(values[i]);
			}
		} else {
			throw new UnsupportedOperationException("Unsupport storage type ");
		}
	}

	public static IntFloatVector deserialize(ByteBuf input) {
		IntFloatVector feats;
		int dim = input.readInt();
		int len = input.readInt();
		StorageMethod storageMethod = StorageMethod.valuesOf(input.readInt());
		switch (storageMethod) {
			case DENSE: {
				float [] values = new float[len];
				for(int i = 0; i < len; i++) {
					values[i] = input.readFloat();
				}
				feats = VFactory.denseFloatVector(values);
				break;
			}

			case SPARSE: {
				feats = VFactory.sparseFloatVector(dim, len);
				for(int i = 0; i < len; i++) {
					feats.set(input.readInt(), input.readFloat());
				}
				break;
			}

			case SORTED:{
				int [] keys = new int[len];
				float [] values = new float[len];
				for(int i = 0; i < len; i++) {
					keys[i] = input.readInt();
					values[i] = input.readFloat();
				}
				feats = VFactory.sortedFloatVector(dim, keys, values);
				break;
			}

			default:
				throw new UnsupportedOperationException("Unsupport storage type " + storageMethod);
		}

		return feats;
	}

	public static void serialize(IntFloatVector feats, DataOutputStream output) throws IOException {
		output.writeInt(feats.getDim());
		output.writeInt((int) feats.getSize());
		if (feats.isDense()) {
			output.writeInt(StorageMethod.DENSE.getValue());
			float[] values = feats.getStorage().getValues();
			for (int i = 0; i < values.length; i++) {
				output.writeFloat(values[i]);
			}
		} else if (feats.isSparse()) {
			output.writeInt(StorageMethod.SPARSE.getValue());
			ObjectIterator<Int2FloatMap.Entry> iter = feats
					.getStorage().entryIterator();
			while (iter.hasNext()) {
				Int2FloatMap.Entry entry = iter.next();
				output.writeInt(entry.getIntKey());
				output.writeFloat(entry.getFloatValue());
			}
		} else if (feats.isSorted()) {
			output.writeInt(StorageMethod.SORTED.getValue());
			int[] keys = feats.getStorage().getIndices();
			float[] values = feats.getStorage().getValues();
			for (int i = 0; i < keys.length; i++) {
				output.writeInt(keys[i]);
				output.writeFloat(values[i]);
			}
		} else {
			throw new UnsupportedOperationException("Unsupport storage type ");
		}
	}

	public static IntFloatVector deserialize(DataInputStream input) throws IOException {
		IntFloatVector feats;
		int dim = input.readInt();
		int len = input.readInt();
		StorageMethod storageMethod = StorageMethod.valuesOf(input.readInt());
		switch (storageMethod) {
			case DENSE: {
				float [] values = new float[len];
				for(int i = 0; i < len; i++) {
					values[i] = input.readFloat();
				}
				feats = VFactory.denseFloatVector(values);
				break;
			}

			case SPARSE: {
				feats = VFactory.sparseFloatVector(dim, len);
				for(int i = 0; i < len; i++) {
					feats.set(input.readInt(), input.readFloat());
				}
				break;
			}

			case SORTED:{
				int [] keys = new int[len];
				float [] values = new float[len];
				for(int i = 0; i < len; i++) {
					keys[i] = input.readInt();
					values[i] = input.readFloat();
				}
				feats = VFactory.sortedFloatVector(dim, keys, values);
				break;
			}

			default:
				throw new UnsupportedOperationException("Unsupport storage type " + storageMethod);
		}

		return feats;
	}

	public static void serialize(float [] feats, ByteBuf output) {
		if(feats != null) {
			output.writeInt(feats.length);
			for(int i = 0; i < feats.length; i++) {
				output.writeFloat(feats[i]);
			}
		} else {
			output.writeInt(0);
		}
	}
	public static void serialize(double [] feats, ByteBuf output) throws IOException {
		if(feats != null) {
			output.writeInt(feats.length);
			for(int i = 0; i < feats.length; i++) {
				output.writeDouble(feats[i]);
			}
		} else {
			output.writeInt(0);
		}
	}



	public static void serialize(float [] feats, DataOutputStream output) throws IOException {
		if(feats != null) {
			output.writeInt(feats.length);
			for(int i = 0; i < feats.length; i++) {
				output.writeFloat(feats[i]);
			}
		} else {
			output.writeInt(0);
		}
	}

	public static void serialize(double [] feats, DataOutputStream output) throws IOException {
		if(feats != null) {
			output.writeInt(feats.length);
			for(int i = 0; i < feats.length; i++) {
				output.writeDouble(feats[i]);
			}
		} else {
			output.writeInt(0);
		}
	}


	public static float[] deserializeFloats(DataInputStream input) throws IOException {
		int len = input.readInt();
		if(len > 0) {
			float[] res = new float[len];
			for(int i = 0; i < len; i++) {
				res[i] = input.readInt();
			}
			return res;
		} else {
			return null;
		}
	}

	public static float[] deserializeFloats(ByteBuf input) {
		int len = input.readInt();
		if(len > 0) {
			float[] res = new float[len];
			for(int i = 0; i < len; i++) {
				res[i] = input.readFloat();
			}
			return res;
		} else {
			return null;
		}
	}

	public static double[] deserializeDoubles(ByteBuf input) {
		int len = input.readInt();
		if(len > 0) {
			double[] res = new double[len];
			for(int i = 0; i < len; i++) {
				res[i] = input.readDouble();
			}
			return res;
		} else {
			return null;
		}
	}

	public static int dataLen(float[] feats) {
		return 4 + (feats == null ? 0 : feats.length * 4);
	}

	public static int dataLen(double[] feats) {
		return 4 + (feats == null ? 0 : feats.length * 8);
	}
}
