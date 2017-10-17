/*
 * CS6350 BIG DATA ANALYTICS AND MANAGEMENT
 * HOMEWORK 1
 * Name: Rammurthy Mudimadugula
 * UTID: rxm163730
 *
 * Class to find the friends with top ten mutual friends.
 */

import java.util.*;
import java.util.Set;
import java.util.Map;
import java.util.TreeMap;
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
public class TopTen {
	
	/*
	 * Array writable text class.
	 */
	public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
        
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
	 *
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
	 *
	 */
	public static class Reduce extends Reducer<Text,TextArrayWritable,Text,Text> {
		public static String[] sarray1;
		public static String[] sarray2;

		private Text contextKey = new Text(); // type of output key
		private Text contextValue = new Text(); // type of output key
		
		private TreeMap<Integer, ArrayList<String>> tm = new TreeMap<>();

		/*
		 *
		 */
		public void reduce(Text key, Iterable<TextArrayWritable> values,Context context) throws IOException, InterruptedException {
			
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

			String tempString = sb.toString();
			String[] tempStringArray = tempString.split(",");

			if (tempStringArray.length > 0) {

				if (tm.get(tempStringArray.length) == null) {
					ArrayList<String> list = new ArrayList<String>();
					list.add(key.toString() + " " + tempString);
					tm.put(tempStringArray.length, list);
				} else {
					ArrayList<String> list = tm.get(tempStringArray.length);
					list.add(key.toString() + " " + tempString);
					tm.put(tempStringArray.length, list);
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
			Set<Integer> keySet = tm.keySet();

			int check = 10;
			int index = 0;
			ArrayList<Integer> list = new ArrayList<>(keySet);
			Collections.sort(list, Collections.reverseOrder());

			for(Integer i: list) {
				ArrayList<String> output = tm.get(i);

				for (String s: output) {
					String[] tempArray = s.split("\\s");
					if (tempArray.length > 1) {
						contextKey.set(tempArray[0]);
						contextValue.set(tempArray[1] + " " + i.toString());
						context.write(contextKey, contextValue);
						index++;
						if (index == 10) {
							break;
						}
					}
				}

				check = check - output.size();

				if (check <= 0) {
					break;
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

		if (otherArgs.length != 2) {
			System.err.println("Usage: TopTen <in> <out>");
			System.exit(2);
		}

		// create a job with name "wordcount"
		Job job = new Job(conf, "TopTen");
		job.setJarByClass(TopTen.class);
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
