/**
 * Degree.java
 * <p>
 * This class contains Map and Reduce functions for get all the number of degrees and the counts.
 * 
 * @version 4/25/2013
 * @author Li Li (li.li@vanderbilt.edu)
 */
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Degree {
  /*
	 * Map class
	 * The input data is records of (startPoint (endPoint1,endPoint2...endPointi))
	 * Map function will generate key-value pairs - (start point, set of end points)
	 */
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString();
			String[] str = text.split("\\t");
			String[] s = str[1].split(",");
			output.collect(new Text(s.length-2+""),new Text(str[0]));
		}
	}

	/*
	 * Reduce class
	 * The input data is key-value pairs from the output of Map function - 
	 * (start point, set of all end points)
	 * Reduce function will count the number of end points for each start point, 
	 * and generate key-value pairs -
	 * (start point, number of end points for the start point)
	 */
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			int count = 0;
			while (values.hasNext()) {
				values.next();
				count++;
			}
			output.collect(key, new Text(count+""));
		}
	}

	/*
	 * main function 
	 * args[0] is the file path of input data
	 * args[1] is the file path of output data
	 */
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Degree.class);
		conf.setJobName("count degree");

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
