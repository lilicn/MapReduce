/**
 * Triangle1.java
 * <p>
 * This class is the first step for finding the number of triangles in graph,
 * containing Map and Reduce functions for getting the key-value pairs (x,y z) 
 * in which xz and yz is edges in the graph.
 * 
 * @version
 * @author Li Li (li.li@vanderbilt.edu)
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Triangle1 {
  /*
	 * Map class
	 * The input data is records of (startPoint endPoint)
	 * The map function will generate the key-value pairs - (startPoint, endPoint)
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
	 * The input data is output data from Map class - (start point, end point)
	 * The reduce function will generate the key-value pairs - (neighbor pairs, root point) 
	 * For instance (x,y z) in which both xz and yz are edges in the graph,
	 * and z < x < y for escaping the repeat triangles
	 * 
	 */
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			ArrayList<String> al = new ArrayList<String>();
			while (values.hasNext()) {
				al.add(values.next().toString());
			}
			int s = Integer.parseInt(key.toString());
			for(int i =0; i<al.size(); i++){
				int si = Integer.parseInt(al.get(i));
				for(int j = i+1; j<al.size(); j++){
					int sj = Integer.parseInt(al.get(j));
					String temp;
					// only output (1,2 0)
					if(si>s && sj>s){
						if(si<sj){
							temp = si+","+sj;
						}else{
							temp = sj+"," +si;
						}
						output.collect(new Text(temp),key);
					}				
				}
			}
		}
	}

	/*
	 * main function
	 * args[0] is the file path of input data
	 * args[1] is the file path of output data
	 */
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Triangle1.class);
		conf.setJobName("Triangle1");

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
