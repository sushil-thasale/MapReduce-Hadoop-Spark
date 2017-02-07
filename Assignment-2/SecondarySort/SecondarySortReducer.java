package SecondarySort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortReducer extends
		Reducer<StationYearPair, SumCountPair, Text, Text> {

	private double tminSum = 0;
	private double tmaxSum = 0;
	private long tminCount = 0;
	private long tmaxCount = 0;
	private StringBuffer summary;
	private String currentYear;

	public void reduce(StationYearPair key, Iterable<SumCountPair> values,
			Context context) throws IOException, InterruptedException {

		summary = new StringBuffer("[");
		currentYear = "";
		clear();
		String stationID = key.getStationID();

		// for a given station
		// compute average TMIN and TMAX for each year
		for (SumCountPair sp : values) {

			// start of new year for same station
			if (!currentYear.equals(key.getYear())) {

				// append current year's data to station's summary
				if (!currentYear.equals("")) {
					updateSummary();
					clear();
				}

				// update current year and start data accumulation
				currentYear = key.getYear();
				accumulate(sp);
			} else {

				accumulate(sp);
			}
		}

		updateSummary();		
		context.write(new Text(stationID), new Text(summary.toString()));
	}

	/*
	 * compute average and return it
	 */
	private String getAverage(double sum, long count) {

		return (count == 0 ? "None" : "" + (double) (sum / count));
	}

	/*
	 * clear accumulators
	 * initialize all accumulators to 0, before start of new year
	 */
	private void clear() {
		tminSum = 0;
		tmaxSum = 0;
		tminCount = 0;
		tmaxCount = 0;
	}

	/*
	 * append average TMIN and TMAX of current year to summary
	 */
	private void updateSummary() {
		summary.append("(" + currentYear + " : "
				+ getAverage(tminSum, tminCount) + " , "
				+ getAverage(tmaxSum, tmaxCount) + "), ");
	}

	/*
	 * update all accumulators to get total sum and count for TMIN and TMAX
	 */
	private void accumulate(SumCountPair sp) {
		tminSum += sp.getTminSum();
		tmaxSum += sp.getTmaxSum();
		tminCount += sp.getTminCount();
		tmaxCount += sp.getTmaxCount();
	}
}

/*
 * NOTE: CustomeReducer extends Reducer<StationYearPair, Iterable<SumCountPair>,
 * Text, Text> is written instead Reducer<StationYearPair, SumCountPair, Text,
 * Text> then Reducer is not invoked!!!
 */
