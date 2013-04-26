/**
 * Triangle2.java
 * <p>
 * This class is the second step for finding the number of triangles in graph,
 * containing Map and Reduce function for getting all triangles (x,y z),
 * in which xy, yz and xz are all edges in the graph.
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

public class Triangle2 {
  /*
	 * Map class
	 * The input data is the output data from Triangle1.java - (neighbor pairs (x, y), rootPoint (z)) 
	 * and also the original data - (start point,end point,True)
	 * The map function will generate key-value pairs - (startPoint,endPoint True) or (neighborPairs rootPoint)
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
			output.collect(new Text(str[0]), new Text(str[1]));
		}
	}

	/*
	 * Reduce class
	 * The input data is the output data from map function - (startPoint,endPoint True) or (neighborPairs rootPoint)
	 * The reduce function will generate gather all the records with same key. 
	 * If the input set of values for reduce contains both True and some rootPoint),
	 * it will generate the new key-value pair - (neighborPairs rootPoint) 
	 * For instance, (x,y,z)
	 */
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			ArrayList<String> al = new ArrayList<String>();
			boolean boo = false;
			while (values.hasNext()) {
				String current = values.next().toString();
				if(current.equals("true")){
					boo = true;
				}else{
					al.add(current);
				}			
			}
			if(boo){
				for (int i = 0; i < al.size(); i++) {					
					output.collect(new Text(key.toString()+","+al.get(i)), new Text("3"));			
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
		JobConf conf = new JobConf(Triangle2.class);
		conf.setJobName("Triangle2");

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
