package CustomCombiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Aggregates sum and count of TMIN and TMAX
 * NOTE: for Combiner the input is same as Reducer
 * and output is same as Mapper
 */
public class CustomCombiner extends
		Reducer<Text, SumCountPair, Text, SumCountPair> {

	public void reduce(Text key, Iterable<SumCountPair> values, Context context) throws IOException, InterruptedException {
		
		double tminSum = 0;
		double tmaxSum = 0;
		long tminCount = 0;
		long tmaxCount = 0;

		// accumulate sum and count of TMIN and TMAX
		for (SumCountPair sp : values) {
			tminSum += sp.getTminSum();
			tmaxSum += sp.getTmaxSum();
			tminCount += sp.getTminCount();
			tmaxCount += sp.getTmaxCount();
		}

		SumCountPair sp = new SumCountPair();
		sp.setTminSum(tminSum);
		sp.setTmaxSum(tmaxSum);
		sp.setTminCount(tminCount);
		sp.setTmaxCount(tmaxCount);

		context.write(key, sp);
	}
}
