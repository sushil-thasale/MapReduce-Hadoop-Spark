package SecondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 * Grouping Key Comparator for Secondary Sort
 * keys with same stationIDs are considered equal
 */
public class GroupingKeyComparator extends WritableComparator {

	protected GroupingKeyComparator() {
		super(StationYearPair.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		StationYearPair sp1 = (StationYearPair) w1;
		StationYearPair sp2 = (StationYearPair) w2;
		return (sp1.getStationID().compareTo(sp2.getStationID()));
	}
}
