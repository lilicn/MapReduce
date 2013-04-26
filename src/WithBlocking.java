package test;

/**
 * WithBlocking.java
 * <p>
 * This class containing Map and Reduce functions will get the blocks with same attribute 3.
 * 
 * @version 4/25/2013
 * @author Li Li (li.li@vanderbilt.edu)
 */

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WithBlocking {

  /*
	 * Map1 to put records with same attribute 2 into a block
	 */
	public static class Map1 extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			// key = last name
			String current = value.toString();
			String[] cur_strs = current.split(",");
			if(cur_strs.length>4 && cur_strs[3]!=""){
				output.collect(new Text(cur_strs[3]), new Text(cur_strs[0]));
			}		
		}
	}

	/*
	 * Reduce1 to record the block size and id in blocks
	 */
	public static class Reduce1 extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			long size = 0;
			StringBuffer sb = new StringBuffer();
			while (values.hasNext()) {
				String current = values.next().toString();
				sb.append(current + ",");
				size++;
			}
			if (size > 1) {
				output.collect(new Text(sb.toString()), new Text(size + ""));
			}
		}
	}

	/*
	 * main function
	 * args[0] is the file path of input data 
	 * args[1] is the file path of output data
	 */
	public static void main(String[] args) throws Exception {
		// job 1 for MapReduce
		JobConf job1 = new JobConf(WithBlocking.class);
		job1.setJobName("WithBlocking");
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);

		job1.setInputFormat(TextInputFormat.class);
		job1.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		JobClient.runJob(job1);
	}
}
