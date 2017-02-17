package CustomCombiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Reducer program, It aggregates sum and count of TMIN and TMAX
 * and computes average 
 */
public class CustomCombinerReducer extends Reducer<Text, SumCountPair, Text, Text> {

	public void reduce(Text key, Iterable<SumCountPair> values, Context context)
			throws IOException, InterruptedException {

		double tminSum = 0;
		double tmaxSum = 0;
		long tminCount = 0;
		long tmaxCount = 0;

		// aggregate sum and count of TMIN and TMAX
		for (SumCountPair sp : values) {
			tminSum += sp.getTminSum();
			tmaxSum += sp.getTmaxSum();
			tminCount += sp.getTminCount();
			tmaxCount += sp.getTmaxCount();
		}

		// compute average and emit the result
		String avgTmin = getAverage(tminSum, tminCount);
		String avgTmax = getAverage(tmaxSum, tmaxCount);
		Text result = new Text(", " + avgTmin + ", " + avgTmax);

		context.write(key, result);
	}

	private String getAverage(double sum, long count) {

		return (count == 0 ? "None" : "" + (double) (sum / count));
	}
}
