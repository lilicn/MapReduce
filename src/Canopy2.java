/**
 * Canopy2.java
 * <p>
 * This class is the third step for canopy blocking, 
 * which contains one Map and Reduce functions for assigning the nodes into corresponding canopies. 
 *  
 * @version 4/25/2013
 * @author Li Li (li.li@vanderbilt.edu)
 */
import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Canopy2 {

  /*
	 * Map2 class
	 * The input data is same as the Canopy1 - (record id, attribute 1, attribute 2,...,attribute 5)
	 * Besides, each map can access the distributed cache assigned in args[1]
	 * It will generate key-value pairs - (canopy center, node in the canopy)
	 */
	public static class Map2 extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private float t1 = 4;

		private Set<String> cen = new HashSet<String>();

		// this function will be called before the Map function
		public void configure(JobConf job) {
			try {
				Path[] rangeFilePaths = DistributedCache
						.getLocalCacheFiles(job);
				for (Path rangeFilePath : rangeFilePaths) {
					BufferedReader br = new BufferedReader(new FileReader(
							rangeFilePath.toString()));
					String line;
					while ((line = br.readLine()) != null) {
						String temp = line.split(";\\t")[0];
						String[] strs = temp.split(";");
						for (int i = 0; i < strs.length; i++) {
							cen.add(strs[i]);
						}
					}
					br.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			// add records into canopy
			String str_temp = value.toString();
			if (!cen.contains(str_temp)) {
				Iterator<String> it = cen.iterator();
				while (it.hasNext()) {
					String s2 = it.next();
					if (simpleDist(str_temp, s2) < t1) {
						output.collect(new Text(s2.split(",")[0]), new Text(
								str_temp.split(",")[0]));
					}
				}
			}
		}

		public float simpleDist(String s1, String s2) {
			float min = Math.abs(s1.length() - s2.length());
			return min;
		}
	}

	/*
	 * Reduce2 class
	 * The input data is the output data from Map2 - (canopy center, node in the canopy)
	 * Reduce function will generate key-value pairs - (canopy center, set of node in this canopy)
	 */
	public static class Reduce2 extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			long size = 1;
			output.collect(new Text(size + ":"), key);
			while (values.hasNext()) {
				size++;
				String current = values.next().toString();
				output.collect(new Text(size + ":"), new Text(current));
			}
		}
	}

	/*
	 * main function 
	 * args[0] is the file path of input data 
	 * args[1] is the file path of distributed cache
	 * args[2] is the file path of output data
	 */
	public static void main(String[] args) throws Exception {
		JobConf job2 = new JobConf(Canopy2.class);
		job2.setJobName("Canopy2");

		// according the number of the output data in the last step, 
		// we may add several distributed cache files
		DistributedCache.addCacheFile(new URI(args[1] + "part-00000"), job2);
		DistributedCache.addCacheFile(new URI(args[1] + "part-00001"), job2);
		DistributedCache.addCacheFile(new URI(args[1] + "part-00002"), job2);
		DistributedCache.addCacheFile(new URI(args[1] + "part-00003"), job2);
		DistributedCache.addCacheFile(new URI(args[1] + "part-00004"), job2);
		DistributedCache.addCacheFile(new URI(args[1] + "part-00005"), job2);
		DistributedCache.addCacheFile(new URI(args[1] + "part-00006"), job2);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);

		// we may set the number of reduce tasks
		//job2.setNumReduceTasks(9);

		job2.setInputFormat(TextInputFormat.class);
		job2.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		JobClient.runJob(job2);
	}
}
