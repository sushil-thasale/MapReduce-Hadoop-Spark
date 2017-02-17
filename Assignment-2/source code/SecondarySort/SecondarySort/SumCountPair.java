package SecondarySort;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/*
 * Custom class to store TMIN and TMAX data
 * Note: Custom key or value class must implements WritableComparable
 * and any class that implements this interface need to implement 3 abstract methods
 * (write, readFields and compareTo) 
 */
public class SumCountPair implements WritableComparable<SumCountPair> {

	private double tminSum;
	private double tmaxSum;
	private long tminCount;
	private long tmaxCount;

	public SumCountPair() {
		this.tminSum = 0;
		this.tmaxSum = 0;
		this.tminCount = 0;
		this.tmaxCount = 0;
	}

	public void addTmin(double value) {
		tminSum += value;
		tminCount++;
	}

	public void addTmax(double value) {
		tmaxSum += value;
		tmaxCount++;
	}

	public double getTminSum() {
		return tminSum;
	}

	public double getTminCount() {
		return tminCount;
	}

	public double getTmaxSum() {
		return tmaxSum;
	}

	public double getTmaxCount() {
		return tmaxCount;
	}

	public void setTminSum(double tminSum) {
		this.tminSum = tminSum;
	}

	public void setTminCount(long tminCount) {
		this.tminCount = tminCount;
	}

	public void setTmaxSum(double tmaxSum) {
		this.tmaxSum = tmaxSum;
	}

	public void setTmaxCount(long tmaxCount) {
		this.tmaxCount = tmaxCount;
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		this.tminSum = in.readDouble();
		this.tmaxSum = in.readDouble();
		this.tminCount = in.readLong();
		this.tmaxCount = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeDouble(tminSum);
		out.writeDouble(tmaxSum);
		out.writeLong(tminCount);
		out.writeLong(tmaxCount);
	}

	/*
	 * Since SumCountPair won't be used as key, no need to define this function
	 */
	@Override
	public int compareTo(SumCountPair sp) {
		return 0;
	}
}
