/**
 * BFS.java
 * <p>
 * This class contains Map and Reduce functions for finding single source shortest path 
 * which can be seen as breadth first search tree. 
 * Before the use of this class, Initial.java will be applied firstly.
 * 
 * @version 4/25/2013
 * @author Li Li (li.li@vanderbilt.edu)
 */

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class BFS {

  /*
	 * Map class
	 * The input data is from the output data of Initial.java - 
	 * (start point, color (Gray or White), distance (0 or Inf), set of all end points)
	 * If the color of record is gray, Map function will change the color to Black and generate it,
	 * and also generate the end points with the distance + 1 and Gray.
	 * If the color is White, it just generate the key-values pairs - 
	 * (start point, color (Gray or White), distance (0 or Inf), set of all end points)
	 */
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString();
			String[] s = text.split("\\t");
			String[] str = s[1].split(",");
			if (str[0].equals("Gray")) {
				StringBuffer sb = new StringBuffer();
				sb.append("Black," + str[1]);
				for (int i = 2; i < str.length; i++) {
					sb.append("," + str[i]);
					int index = Integer.parseInt(str[1]) + 1;
					output.collect(new Text(str[i]), new Text("Gray," + index));
				}
				output.collect(new Text(s[0]), new Text(sb.toString()));
			} else {
				output.collect(new Text(s[0]), new Text(s[1]));
			}
		}
	}

	/*
	 * Reduce class
	 * The input data is key-value pairs from the output of Map function - 
	 * (start point, color (Gray or White or Black), distance, set of all end points)
	 * The record with same start point will be assigned in the same reduce. 
	 * If there are records with both Gray and White color, Reduce function will generate key-value pairs -
	 * (start point, Gray, distance (in the record with Gray), set of all end points (in the record with White)
	 * If there is record with Black, Reduce will only output the record with Black.
	 * If there is only record with White or Black, reduce will only output the record.
	 */
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			int index = -1;
			String color = "White";
			boolean boo = true;
			StringBuffer sb = new StringBuffer();
			while (values.hasNext()) {
				String current = values.next().toString();
				String[] strs = current.split(",");
				if (strs[0].equals("Black")) {
					output.collect(key, new Text(current));
					boo = false;
				} else if (strs[0].equals("Gray")) {
					index = Integer.parseInt(strs[1]);
					color = "Gray";
				} else {
					for (int i = 2; i < strs.length; i++) {
						sb.append("," + strs[i]);
					}
				}
			}
			if (boo)
				output.collect(key,
						new Text(color + "," + index + sb.toString()));
		}
	}

	/*
	 * main function
	 * args[0] is the file path of input data
	 * args[1] is the file path of output data
	 */
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(BFS.class);
		conf.setJobName("BFS");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
