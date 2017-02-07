package InMapperCombiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Reducer : compute average TMIN and TMAX for each station and emits it 
 */
public class InMapperCombinerReducer extends Reducer<Text, SumCountPair, Text, Text> {

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

		// compute average and emit it
		String avgTmin = getAverage(tminSum, tminCount);
		String avgTmax = getAverage(tmaxSum, tmaxCount);
		Text result = new Text(", " + avgTmin + ", " + avgTmax);

		context.write(key, result);
	}

	// computes average and returns it as String
	// returns "None" if TMIN or TMAX doesn't exist for any station
	private String getAverage(double sum, long count) {

		return (count == 0 ? "None" : "" + (double) (sum / count));
	}
}
