/**
 * 
 */
package com.hadoopbasics.learnings;

import java.io.IOException;
import java.net.URI;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

/**
 * @author Mahesh Deshmukh
 *
 */
public class MaxTempExample extends Configured implements Tool {
	final static Logger logger = Logger.getLogger("MyLogger");
	
	public static void main(String[] args) throws Exception {
		String log4jConfPath = "log4j.properties";
		PropertyConfigurator.configure(log4jConfPath);

		logger.info("Logging Started : ....");
		if(args.length != 2){
			System.err.println("Usage: MaxTemperature <input path> <output path>");
			System.exit(-1);
		}
		int res = ToolRunner.run(new MaxTempExample(), args);
		System.exit(res);

	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MaxTempExample");
		
		job.setJarByClass(MaxTempExample.class);
		
		job.setMapperClass(MaxTempMapper.class);
		job.setReducerClass(MaxTempReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);		
		

		
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath);
		FileOutputFormat.setOutputPath(job, outputPath);		
		
		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

}

class MaxTempMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private static final int MISSING = 9999;
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
		throws IOException, InterruptedException {
		
		String line = value.toString();
		String year = line.substring(15, 19);
		int airTemperature;
		if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
		airTemperature = Integer.parseInt(line.substring(88, 92));
		} else {
		airTemperature = Integer.parseInt(line.substring(87, 92));
		}
		String quality = line.substring(92, 93);
		
		if (airTemperature != MISSING && quality.matches("[01459]")) {
			context.write(new Text(year), new IntWritable(airTemperature));
			}
		
	}
	
}

class MaxTempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int maxValue = Integer.MIN_VALUE;
		for (IntWritable value : values) {
			maxValue = Math.max(maxValue, value.get());
		}
		context.write(key, new IntWritable(maxValue));
	}
}




