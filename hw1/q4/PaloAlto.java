/*
 * CS6350 BIG DATA ANALYTICS AND MANAGEMENT
 * HOMEWORK 1
 * Name: Rammurthy Mudimadugula
 * UTID: rxm163730
 *
 * Class to find the users who rated businesses in Palo Alto.
 */

import java.io.*;
import java.util.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

@SuppressWarnings("deprecation")
public class PaloAlto {
	
	/*
	 *
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private Text userId     = new Text(); // type of output key
		private Text reviewInfo = new Text();

		private Set<String> addr = new HashSet<String>();

		/*
		 *
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			URI[] fList = context.getCacheFiles();
			if (fList.length == 1) {
				rFile(fList[0]);
			}
		}

		/*
		 *
		 */
		private void rFile(URI fPath) throws IOException, FileNotFoundException {
			Path path = new Path(fPath);
			BufferedReader br = new BufferedReader(new FileReader(path.getName()));
			String line = null;
			String[] temp;

			while ((line = br.readLine()) != null) {
				temp = line.split("::");
				if (temp.length == 3) {
					if (temp[1].toLowerCase().contains("palo alto")) {
						addr.add(temp[0]);
					}
				}
			}
			br.close();
		}

		/*
		 *
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] temp = value.toString().split("::");
			if (temp.length == 4) {
				if (addr.contains(temp[2])) {
					userId.set(temp[1]);
					reviewInfo.set(temp[3]);
					context.write(userId, reviewInfo);
				}
			}
		}
	}

	/*
	 *
	 */
	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		private Text avgRating = new Text();
		
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			Double sum = 0.0;
			int count = 0;

			for (Text t: values) {
				sum += Double.parseDouble(t.toString());
				count += 1;
			}

			Double avg = (Double) sum/count;
			avgRating.set(Double.toString(avg));
			context.write(key, avgRating);
		}
	}

	/*
	 * Driver program.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err.println("Usage: PaloAlto <review.csv> <business.csv> <out>");
			System.exit(2);
		}

		// create a job with name "wordcount"
		Job job = new Job(conf, "PaloAlto");
		job.setJarByClass(PaloAlto.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.addCacheFile(new Path(otherArgs[1]).toUri());
		// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
