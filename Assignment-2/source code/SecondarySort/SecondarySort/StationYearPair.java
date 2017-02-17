package SecondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class StationYearPair implements WritableComparable<StationYearPair> {

	private String stationID;
	private String year;

	public StationYearPair() {
	}

	public StationYearPair(String statinID, String year) {
		this.stationID = statinID;
		this.year = year;
	}

	public String getStationID() {
		return stationID;
	}

	public String getYear() {
		return year;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.stationID = in.readUTF();
		this.year = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(stationID);
		out.writeUTF(year);
	}

	/*
	 * sort in ascending order of stationID then sort in ascending order of year
	 */
	@Override
	public int compareTo(StationYearPair other) {

		if (!this.stationID.equals(other.stationID))
			return (this.stationID.compareTo(other.stationID));
		else
			return (this.year.compareTo(other.year));
	}

	/*
	 * NOTE: when custom objects are used as keys in HashMap, the hashcode() is
	 * called to check equality of two keys if both keys have same hashcode,
	 * then equals() method is called if equals() return true then
	 * HashMap.containsKey() returns true
	 */
	@Override
	public int hashCode() {
		return (stationID + "\t" + year).hashCode();
	}

	/*
	 * If two keys have same ststionID and Year, then they are considered equal
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		
		StationYearPair other = (StationYearPair) obj;
		return (this.stationID.equals(other.stationID) && this.year
				.equals(other.year));
	}

	@Override
	public String toString() {
		return (this.stationID + "\t" + this.year);
	}
}
