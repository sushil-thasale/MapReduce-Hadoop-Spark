package NoCombiner;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Mapper: Emit TMIN and TMAX for each station without aggregating
 */
public class NoCombinerMapper extends
		Mapper<LongWritable, Text, Text, SumCountPair> {

	public void map(LongWritable offset, Text line, Context context)
			throws IOException, InterruptedException {

		// parse the line and extract stationID and temperature value
		String contents[] = line.toString().split(",");
		String station = contents[0].trim();
		Text stationID = new Text(station);
		Double temperature = Double.parseDouble(contents[3].trim());
		SumCountPair sp = new SumCountPair();

		// check if line corresponds to TMIN or TMAX
		if (contents[2].trim().equals("TMAX")) {
			sp.addTmax(temperature);
			context.write(stationID, sp);
		} else if (contents[2].trim().equals("TMIN")) {
			sp.addTmin(temperature);
			context.write(stationID, sp);
		}		
	}
}
