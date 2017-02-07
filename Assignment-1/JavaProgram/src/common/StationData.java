package src.common;

/*
 * StationData : Data Structure for storing data of each station
 */
public class StationData {

	private double sum;
	private long count;

	public StationData() {
		sum = 0;
		count = 0;
	}

	public double getAverage() {
		return ((double) sum / count);
	}

	public void incrementSum(double tmax) {
		sum += tmax;
	}

	public void incrementCount(long n) {
		count += n;
	}

	public double getSum() {
		return sum;
	}

	public long getCount() {
		return count;
	}
	
	public StationData clone()
	{
		StationData s = new StationData();
		s.count = this.count;
		s.sum = this.sum;
		return s;				
	}
}
