package NoCombiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Reducer : aggregate sum and count for each station and emit average
 */
public class NoCombinerReducer extends Reducer<Text, SumCountPair, Text, Text> {

	public void reduce(Text key, Iterable<SumCountPair> values, Context context)
			throws IOException, InterruptedException {

		double tminSum = 0;
		double tmaxSum = 0;
		long tminCount = 0;
		long tmaxCount = 0;

		// aggregate sum and count for given station
		for (SumCountPair sp : values) {
			tminSum += sp.getTminSum();
			tmaxSum += sp.getTmaxSum();
			tminCount += sp.getTminCount();
			tmaxCount += sp.getTmaxCount();
		}

		// compute average and emit
		String avgTmin = getAverage(tminSum, tminCount);
		String avgTmax = getAverage(tmaxSum, tmaxCount);
		Text result = new Text(", " + avgTmin + ", " + avgTmax);

		context.write(key, result);
	}

	/*
	 * computes average TMIN and TMAX
	 * returns "None" if any station doesn't have TMIN or TMAX values
	 */
	private String getAverage(double sum, long count) {

		return (count == 0 ? "None" : "" + (double) (sum / count));
	}
}
