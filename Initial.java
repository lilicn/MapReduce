/**
 * Initial.java
 * <p>
 * This class contains Map and Reduce functions for the initial part 
 * before the use of BFS.java. 
 * It will assign colors for each node.
 * 
 * @version 4/25/2013
 * @author Li Li (li.li@vanderbilt.edu)
 */
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Initial {
  /*
	 * Map class 
	 * The input of Map function is records of (startPoint endPoint)
	 * Map function will generate key-value pairs - (start point, set of all end Points)
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
			output.collect(new Text(str[0]),new Text(str[1]));
		}
	}
	
	/*
	 * Reduce class
	 * The input data is key-value pairs from the output of Map function - 
	 * (start point, set of all end points)
	 * Reduce function will generate key-value pairs -
	 * (start point, color (Gray or White), distance (0 or Inf), set of all end points)
	 */
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		String start;
		// this function will be called before the Reduce function
		public void configure(JobConf job) {
			start = job.get("start");
		}
		
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			StringBuffer sb = new StringBuffer();
			String current;
			if(key.toString().equals(start)){
				sb.append("Gray,0");
			}else{
				sb.append("White,-1");
			}			
			while (values.hasNext()) {
				current = values.next().toString();
				sb.append(","+current);			
			}
			output.collect(key, new Text(sb.toString()));
		}
	}

	/*
	 * main function
	 * args[0] is the file path of input data
	 * args[1] is the file path of output data
	 * args[2] is the node which will be assigned as gray node - root of the BFS tree
	 */
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Initial.class);
		conf.setJobName("BFS_initial");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		conf.set("start", args[2]);
		JobClient.runJob(conf);
	}
}
