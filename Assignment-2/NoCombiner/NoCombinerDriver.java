package NoCombiner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * Driver program
 */
public class NoCombinerDriver {

	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

		if (args.length < 2) {
			System.out.println("Provide input and output directories");
			System.exit(1);
		}

		String inputDir = args[0];
		String outputDir = args[1];

		Configuration conf = new Configuration();
		Job job = new Job(conf, "No Combiner Implementation");
		job.setJarByClass(NoCombinerDriver.class);
		job.setMapperClass(NoCombinerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(SumCountPair.class);
		job.setReducerClass(NoCombinerReducer.class);
		job.setNumReduceTasks(5);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
