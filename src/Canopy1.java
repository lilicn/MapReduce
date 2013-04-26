/**
 * Canopy1.java
 * <p>
 * This class is the first step for canopy blocking, 
 * which contains one Map and Reduce functions for finding the initial canopy centers. 
 *  
 * @version 4/25/2013
 * @author Li Li (li.li@vanderbilt.edu)
 */
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class Canopy1 {
  /*
	 * Map1 class
	 * The input data is records of (record id, attribute 1, attribute 2,...,attribute 5)
	 * The map class will get all the records, and output the canopy centers
	 */
	public static class Map1 extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private float t2 = 10;
		private ArrayList<String> records = new ArrayList<String>();

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String current = value.toString();
			boolean boo = true;
			for (int j = 0; j < records.size(); j++) {
				if (simpleDist(records.get(j), current) < t2) {
					boo = false;
					break;
				}
			}
			if (boo) {
				// only store the records with distance > l2
				output.collect(new Text("center"), new Text(current));
				records.add(current);
			}
		}

		public float simpleDist(String s1, String s2) {
			String[] s1_strs = s1.split(",");
			String[] s2_strs = s2.split(",");
			float min = 0;
			// this is the similarity function for group 2
			for (int i = 1; i < 6; i++) {
				if (!s1_strs[i].equals(s2_strs[i])) {
					min += 1;
					min += Math.abs(s1_strs[i].length() - s2_strs[i].length());
				}
			}
			// this is the simplest similarity function for group 1
			// float min = Math.abs(s1.length()-s2.length());
			return min;
		}
	}

	/*
	 * Reduce class
	 * The reduce function will get final canopy centers in just one Reducer.
	 */
	public static class Reduce1 extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private float t2 = 10;

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			ArrayList<String> records = new ArrayList<String>();
			StringBuffer sb = new StringBuffer();
			while (values.hasNext()) {
				String current = values.next().toString();
				boolean boo = true;
				for (int j = 0; j < records.size(); j++) {
					if (simpleDist(records.get(j), current) < t2) {
						boo = false;
						break;
					}
				}
				if (boo) {
					records.add(current);
					sb.append(current);
					sb.append(";");
				}
			}
			output.collect(new Text(sb.toString()), new Text(records.size()
					+ ""));
		}

		public float simpleDist(String s1, String s2) {
			String[] s1_strs = s1.split(",");
			String[] s2_strs = s2.split(",");
			float min = 0;
			for (int i = 1; i < 6; i++) {
				if (!s1_strs[i].equals(s2_strs[i])) {
					min += 1;
					min += Math.abs(s1_strs[i].length() - s2_strs[i].length());
				}
			}
			// float min = Math.abs(s1.length()-s2.length());
			return min;
		}
	}

	/*
	 * main function 
	 * args[0] is the file path of input data 
	 * args[1] is the file path of output data
	 */
	public static void main(String[] args) throws Exception {
		JobConf job1 = new JobConf(Canopy1.class);
		job1.setJobName("Canopy1");
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
