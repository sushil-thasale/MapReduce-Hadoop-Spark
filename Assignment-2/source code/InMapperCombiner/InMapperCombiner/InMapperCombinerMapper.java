package InMapperCombiner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InMapperCombinerMapper extends
		Mapper<LongWritable, Text, Text, SumCountPair> {

	private HashMap<String, SumCountPair> combiner;

	// using a HAshMap to aggregate sum and count of TMIN and TMAX
	public void setup(Context context) {
		combiner = new HashMap<String, SumCountPair>();
	}

	public void map(LongWritable offset, Text line, Context context)
			throws IOException, InterruptedException {

		// parse the line and extract stationID and temperature value
		String contents[] = line.toString().split(",");
		String station = contents[0].trim();
		double temperature = Double.parseDouble(contents[3].trim());

		// check if line corresponds to TMIN or TMAX
		if (contents[2].trim().equals("TMAX")) {

			// aggregate TMIN and TMAX sum using HashMap 
			if (combiner.containsKey(station)) {
				combiner.get(station).addTmax(temperature);
			} else {
				SumCountPair sp = new SumCountPair();
				sp.addTmax(temperature);
				combiner.put(station, sp);
			}
		} else if (contents[2].trim().equals("TMIN")) {

			if (combiner.containsKey(station)) {
				combiner.get(station).addTmin(temperature);
			} else {
				SumCountPair sp = new SumCountPair();
				sp.addTmin(temperature);
				combiner.put(station, sp);
			}
		}
	}

	/*
	 * emit the aggregated values for each statioID
	 */
	public void cleanup(Context context) throws IOException, InterruptedException {

		for (Map.Entry<String, SumCountPair> entry : combiner.entrySet()) {
			
			Text stationID = new Text(entry.getKey());
			System.out.println(stationID.toString());
			context.write(stationID, entry.getValue());
		}
	}
}
