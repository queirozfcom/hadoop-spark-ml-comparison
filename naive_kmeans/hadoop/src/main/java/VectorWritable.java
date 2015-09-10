import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import java.util.Arrays;
import java.util.Vector;

public final class VectorWritable implements WritableComparable<VectorWritable> {

	private Vector<Double> vector;

	public VectorWritable() {
		super();
	}

	public VectorWritable(VectorWritable v) {
		this.vector = v.getVector();
	}

	public VectorWritable(Vector<Double> v) {
		this.vector = v;
	}

	/*
	 * Some oldschool compatibility convenience constructors
	 */

	public VectorWritable(double x) {
		double[] arr = {x};
		this.vector = new Vector<Double>(Utils.arrayToVector(arr));
	}

	public VectorWritable(double x, double y) {
		double[] arr = {x,y};
		this.vector = new Vector<Double>(Utils.arrayToVector(arr));
	}

	public VectorWritable(double[] arr) {
		this.vector = new Vector<Double>(Utils.arrayToVector(arr));
	}

	@Override
	public final void write(DataOutput out) throws IOException {
		writeVector(this.vector, out);
	}

	@Override
	public final void readFields(DataInput in) throws IOException {
		this.vector = readVector(in);
	}

	@Override
	public final int compareTo(VectorWritable o) {
		return compareVector(this, o);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((vector == null) ? 0 : vector.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		VectorWritable other = (VectorWritable) obj;
		if (vector == null) {
			if (other.vector != null)
				return false;
		} else if (!vector.equals(other.vector))
			return false;
		return true;
	}

	/**
	 * @return the vector
	 */
	public Vector<Double> getVector() {
		return vector;
	}

	@Override
	public String toString() {
		return vector.toString();
	}

	public static void writeVector(Vector<Double> vector, DataOutput out) throws IOException {
		out.writeInt(vector.size());
		for (int i = 0; i < vector.size(); i++) {
			out.writeDouble(vector.get(i));
		}
	}

	public static Vector<Double> readVector(DataInput in) throws IOException {
		final int length = in.readInt();

		Vector<Double> vector = new Vector<Double>();

		for (int i = 0; i < length; i++) {
			vector.add(in.readDouble());
		}
		return vector;
	}

	public static int compareVector(VectorWritable a, VectorWritable o) {
		return compareVector(a.getVector(), o.getVector());
	}

	public static int compareVector(Vector<Double> a, Vector<Double> o) {
		Vector<Double> subtract = Utils.subtract(a,o);
		
		double sum = Utils.sum(subtract);

		return (int) sum;
	}

	public static VectorWritable wrap(Vector<Double> a) {
		return new VectorWritable(a);
	}

}