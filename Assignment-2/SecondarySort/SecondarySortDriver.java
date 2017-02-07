package SecondarySort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * Driver program for Secondary sort
 */
public class SecondarySortDriver {

	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length < 2) {
			System.out.println("Please provide input and output path");
			System.exit(1);
		}

		String inputDir = args[0];
		String outputDir = args[1];

		Configuration conf = new Configuration();
		Job job = new Job(conf, "Secondary sort implementation");
		job.setJarByClass(SecondarySortDriver.class);
		job.setMapperClass(SecondarySortMapper.class);
		job.setMapOutputKeyClass(StationYearPair.class);
		job.setMapOutputValueClass(SumCountPair.class);
		job.setGroupingComparatorClass(GroupingKeyComparator.class);
		job.setReducerClass(SecondarySortReducer.class);
		job.setNumReduceTasks(5);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
