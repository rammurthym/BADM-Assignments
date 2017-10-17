/*
 * CS6350 BIG DATA ANALYTICS AND MANAGEMENT
 * HOMEWORK 1
 * Name: Rammurthy Mudimadugula
 * UTID: rxm163730
 *
 * Hadoop Map Reduce job to output common friends between to users.
 */

import java.io.IOException;

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
public class CommonFriends {
	public static String userKey;
		
	/*
	 * Array writable text class.
	 */
	public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        /*
		 * Constructor
		 */
        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
        
        /*
		 * toString method to print the class instance.
		 */
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            int index = 0;
            for (String s : super.toStrings()) {
            	if (index == 0) {
            		sb.append(s);
            	} else {
            		sb.append(",").append(s);
            	}
            	index++;
            }
            return sb.toString();
        }
    }
	
	/*
	 * Map class to find the common friends.
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, TextArrayWritable>{
		private Text word = new Text(); // type of output key

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] temp = value.toString().split("\\t");
			if (temp.length == 2) {
				String[] friends = temp[1].split(",");
				TextArrayWritable taw = new TextArrayWritable(friends);

				for(String s: friends){
					String k;
					if (Integer.parseInt(temp[0]) < Integer.parseInt(s)) {
						k = temp[0] + "," + s;
					} else {
						k = s + "," + temp[0];
					}
					word.set(k);
					context.write(word, taw);
				}
			}
		}
	}

	/*
	 * Reduce class to find the common friends.
	 */
	public static class Reduce extends Reducer<Text,TextArrayWritable,Text,Text> {
		public static String[] sarray1;
		public static String[] sarray2;
		
		public void reduce(Text key, Iterable<TextArrayWritable> values,Context context) throws IOException, InterruptedException {
			
			if (userKey.compareTo(key.toString()) == 0) {		
				int index = 0;
				for(ArrayWritable f:values){
					if (index == 0) {
						sarray1 = f.toString().split(",");
					} else {
						sarray2 = f.toString().split(",");
					}
					index++;
				}
				index = 0;
				StringBuilder sb = new StringBuilder();
				
				for (String s1: sarray1) {
					for (String s2: sarray2) {
						if (s1.compareTo(s2) == 0) {
							if (index == 0) {
								sb.append(s1);
							} else {
								sb.append(","+s1);
							}
							index++;
							break;
						}
					}
				}
				context.write(key,new Text(sb.toString()));
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
			System.err.println("Usage: CommonFriends <in> <out> <user1> <user2>");
			System.exit(2);
		}
		if (Integer.parseInt(otherArgs[2]) < Integer.parseInt(otherArgs[3])) {
			userKey = otherArgs[2] + "," + otherArgs[3];
		} else {
			userKey = otherArgs[3] + "," + otherArgs[2];
		}

		// create a job with name "wordcount"
		Job job = new Job(conf, "CommonFriends");
		job.setJarByClass(CommonFriends.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(TextArrayWritable.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
