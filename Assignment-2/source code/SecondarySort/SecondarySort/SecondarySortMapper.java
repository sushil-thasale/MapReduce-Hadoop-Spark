package SecondarySort;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Mapper for Secondary Sort program
 */
public class SecondarySortMapper extends
		Mapper<LongWritable, Text, StationYearPair, SumCountPair> {

	private HashMap<StationYearPair, SumCountPair> combiner;

	public void setup(Context context) {
		combiner = new HashMap<StationYearPair, SumCountPair>();
	}

	/*
	 * Using In-Mapper combiner for efficiency
	 */
	public void map(LongWritable offset, Text line, Context context) {
		
		// parse the input line
		String contents[] = line.toString().split(",");
		String stationId = contents[0].trim();
		String year = contents[1].trim().substring(0, 4);
		double temperature = Double.parseDouble(contents[3].trim());
		StationYearPair stationYear = new StationYearPair(stationId, year);		

		// accumulate temperatures only if it belongs to TMIN or TMAX
		if (contents[2].trim().equals("TMIN")) {
			if (combiner.containsKey(stationYear)) {
				combiner.get(stationYear).addTmin(temperature);
			} else {
				SumCountPair sp = new SumCountPair();
				sp.addTmin(temperature);
				combiner.put(stationYear, sp);
			}
		}
		if (contents[2].trim().equals("TMAX")) {
			if (combiner.containsKey(stationYear)) {
				combiner.get(stationYear).addTmax(temperature);
			} else {
				SumCountPair sp = new SumCountPair();
				sp.addTmax(temperature);
				combiner.put(stationYear, sp);
			}
		}
	}
	
	/*
	 * Emit all Station-year pairs with respective aggregated values 
	 */
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		for(Map.Entry<StationYearPair, SumCountPair> entry : combiner.entrySet())
		{
			context.write(entry.getKey(), entry.getValue());
		}
	}
}
