package test;

/**
 * Canopy12.java
 * <p>
 * This class is the second step for canopy blocking, 
 * which contains one Map and Reduce functions for finding the missing canopy centers in the first step. 
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

public class Canopy12 {

  /*
	 * Map12 class The input data is same as the Canopy1 - (record id, attribute
	 * 1, attribute 2,...,attribute 5) Besides, each map can access the
	 * distributed cache assigned in args[1] It will generate key-value pairs -
	 * (canopy center, node in the canopy)
	 */
	public static class Map12 extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private float t2 = 10;
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
			boolean boo = true;
			if (!cen.contains(str_temp)) {
				Iterator<String> it = cen.iterator();
				while (it.hasNext()) {
					String s2 = it.next();
					if (simpleDist(str_temp, s2) < t2) {
						boo = false;
						break;
					}
				}
			}
			if(boo){
				output.collect(new Text("center"), new Text(str_temp));
			}
		}

		// you can change the simpleDist according to what you need
		public float simpleDist(String s1, String s2) {
			float min = Math.abs(s1.length() - s2.length());
			return min;
		}
	}

	/*
	 * Reduce2 class The input data is the output data from Map2 - (canopy
	 * center, node in the canopy) Reduce function will generate key-value pairs
	 * - (canopy center, set of node in this canopy)
	 */
	public static class Reduce12 extends MapReduceBase implements
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

		// you can change the simpleDist according to what you need
		public float simpleDist(String s1, String s2) {
			float min = Math.abs(s1.length() - s2.length());
			return min;
		}
	}

	/*
	 * main function args[0] is the file path of input data 
	 * args[1] is the file path of distributed cache 
	 * args[2] is the file path of output data
	 */
	public static void main(String[] args) throws Exception {
		JobConf job12 = new JobConf(Canopy12.class);
		job12.setJobName("Canopy12");

		// according the number of the output data in the first step,
		// we may add several distributed cache files
		DistributedCache.addCacheFile(new URI(args[1] + "part-00000"), job12);

		job12.setOutputKeyClass(Text.class);
		job12.setOutputValueClass(Text.class);

		job12.setMapperClass(Map12.class);
		job12.setReducerClass(Reduce12.class);

		// we may set the number of reduce tasks
		// job12.setNumReduceTasks(9);

		job12.setInputFormat(TextInputFormat.class);
		job12.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job12, new Path(args[0]));
		FileOutputFormat.setOutputPath(job12, new Path(args[2]));

		JobClient.runJob(job12);
	}
}
