package SecondarySort;

import org.apache.hadoop.mapreduce.Partitioner;

public class StationPartitioner extends
		Partitioner<StationYearPair, SumCountPair> {

	/*
	 * Decides the reducer for given key based on stationID Note: if you don't
	 * use a partitioner along with grouping comparator, then keys with same
	 * stationID but different year, can go to different reducers. So, always
	 * use a partitioner along with grouping comparator!
	 */
	@Override
	public int getPartition(StationYearPair key, SumCountPair value,
			int numPartitions) {

		// hashcode of string can be negative!!
		return Math.abs(key.getStationID().hashCode() % numPartitions);
	}

}
