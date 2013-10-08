import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TestRemoteSubmitJob {

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		System.out.println("Start . . . .");

//		conf.set("fs.default.name", "hdfs://master:54320");
		conf.set("mapred.job.tracker", "192.168.0.103:54311");

		conf.set("mapreduce.map.class",
				"org.apache.hadoop.examples.WordCount$TokenizerMapper");
		conf.set("mapreduce.reduce.class",
				"org.apache.hadoop.examples.WordCount$IntSumReducer");

		Job job = new Job(conf, "Hadoop Example remote submit Job");
		job.setJarByClass(org.apache.hadoop.examples.WordCount.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		TextInputFormat.addInputPath(job, new Path(
				"hdfs://master:54320/user/hduser/AIS/ANNA/Data/N_B5_data.txt"));
		TextOutputFormat
				.setOutputPath(
						job,
						new Path(
								"hdfs://master:54320/user/hduser/AIS/ANNA/Data/output-client121/"));
		System.out
				.println("---------------------- End conf-----------------------");
		System.out
				.println("---------------------- Submit and Start your Job-----------------------");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
