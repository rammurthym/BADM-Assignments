/*
 * CS6350 BIG DATA ANALYTICS AND MANAGEMENT
 * HOMEWORK 1
 * Name: Rammurthy Mudimadugula
 * UTID: rxm163730
 *
 * Class to find the top ten businesses with highest average user ratings.
 */

import java.util.*;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

@SuppressWarnings("deprecation")
public class ToptenBiz {

	/*
	 *
	 */
	public static class CalcAvg {

		public static class Map extends Mapper<LongWritable, Text, Text, Text> {

			private Text bizId = new Text();
			private Text stars = new Text();

			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				String[] temp = value.toString().split("::");

				if (temp.length == 4) {
					bizId.set(temp[2]);
					stars.set(temp[3]);
					context.write(bizId, stars);
				}

			}
		}

		/*
		 *
		 */
		public static class Reduce extends Reducer<Text, Text, Text, Text> {

			private Text bizId = new Text();
			private Text avg   = new Text();

			/*
			 *
			 */
			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				
				int count = 0;
				double sum = 0;
				for (Text val : values) {
					count += 1;
					sum += Double.parseDouble(val.toString());
				}

				double average = sum/count;

				bizId.set(key.toString());
				avg.set(Double.toString(average));
				context.write(bizId, avg);
			}
		}
	}

	/*
	 *
	 */
	public static class CalcTopTen {

		/*
		 *
		 */
		public static class map1 extends Mapper<LongWritable, Text, Text, Text> {

			private Text bizId = new Text();
			private Text stars = new Text();

			/*
			 *
			 */
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				String[] temp = value.toString().split("\t");

				if (temp.length == 2) {
					bizId.set(temp[0]);
					stars.set("r-" + temp[1]);
					context.write(bizId, stars);
				} 
			}

		}

		/*
		 *
		 */
		public static class map2 extends Mapper<LongWritable, Text, Text, Text> {

			private Text bizId  = new Text();
			private Text addcat = new Text(); 

			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				String[] temp = value.toString().split("::");

				if (temp.length == 3) {
					String tmp = temp[1] + "\t" + temp[2];
					
					bizId.set(temp[0]);
					addcat.set("b-" + tmp);
					context.write(bizId, addcat);
				}
			}

		}
		
		/*
		 *
		 */
		public static class reduce extends Reducer<Text, Text, Text, Text> {

			private Text contextKey = new Text(); // type of output key
			private Text contextValue = new Text(); // type of output key
			private TreeMap<Double, ArrayList<String>> tm = new TreeMap<>();

			/*
			 *
			 */
			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				String addcat     = new String();
				String stars      = new String();
				
				int rcount = 0;
				int bcount = 0;

				for (Text val : values) {
					String[] temp = val.toString().split("-");
					
					if ("r".equals(temp[0])) {
						stars = temp[1];
						rcount += 1;
					} else if ("b".equals(temp[0]) && bcount == 0) { 
						addcat = temp[1];
						bcount += 1;
					}

					if (rcount == 1 && bcount == 1) {
						break;
					}
				}

				if (rcount == 1 && bcount == 1) {

					if (tm.get(Double.parseDouble(stars)) == null) {
						ArrayList<String> list = new ArrayList<String>();
						list.add(key.toString() + "-" + addcat);
						tm.put(Double.parseDouble(stars), list);
					} else {
						ArrayList<String> list = tm.get(Double.parseDouble(stars));
						list.add(key.toString() + "-" + addcat);
						tm.put(Double.parseDouble(stars), list);
					}

					if (tm.size() > 10) {
						tm.remove(tm.firstKey());
					}
				}
			}

			/*
			 *
			 */
			protected void cleanup(Context context) throws IOException, InterruptedException {
				Set<Double> keySet = tm.keySet();

				int check = 10;
				int index = 0;
				ArrayList<Double> list = new ArrayList<>(keySet);
				Collections.sort(list, Collections.reverseOrder());

				for(Double i: list) {
					ArrayList<String> output = tm.get(i);

					for (String s: output) {
						String[] tempArray = s.split("-");
						if (tempArray.length == 3) {
							contextKey.set(tempArray[1]);
							contextValue.set(tempArray[2] + "\t" + Double.toString(i));
							context.write(contextKey, contextValue);
						} else {
							contextKey.set(tempArray[1] + "-" + tempArray[2]);
							contextValue.set(tempArray[3] + "\t" + Double.toString(i));
							context.write(contextKey, contextValue);
						}
						index++;
						if (index == 10) {
							break;
						}
					}

					check = check - output.size();

					if (check <= 0) {
						break;
					}
				}
			}
		}
	}

	/*
	 * Driver program.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 4) {
			System.err.println("Usage: ToptenBiz <review.csv> <business.csv> <intout> <out>");
			System.exit(2);
		}

		Job job1 = new Job(conf, "CalcAvg");
		job1.setJarByClass(ToptenBiz.class);

		job1.setMapperClass(CalcAvg.Map.class);
		job1.setReducerClass(CalcAvg.Reduce.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.addInputPath(job1, new Path(args[0]));
		TextOutputFormat.setOutputPath(job1, new Path(args[2]));

		// System.exit(job1.waitForCompletion(true) ? 0 : 1);
		job1.waitForCompletion(true);
		Job job2 = new Job(conf, "CalcTopTen");
		job2.setJarByClass(ToptenBiz.class);

		job2.setReducerClass(CalcTopTen.reduce.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, CalcTopTen.map1.class);
		MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, CalcTopTen.map2.class);
		TextOutputFormat.setOutputPath(job2, new Path(args[3]));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
