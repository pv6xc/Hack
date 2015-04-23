package edu.umkc.hackathon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class HackDriver  {
	
	/* public int run(String[] arg0) throws Exception {
		//Job job1 = new Job(getConf());
		//conf2.set("value", "20");
		Job job2 = new Job(getConf());
		Configuration conf = job2.getConfiguration();
		int val = 20;
		
	//	job.setMapperClass(.class);
	//	job.setMapOutputKeyClass(IntWritable.class);
	//	job.setMapOutputValueClass(Text.class);
	//	job.setJarByClass(Task14.class);
	//	job.setJobName("UMKC-Hackathon");
	//	job.setNumReduceTasks(0);
		
		
	//	FileInputFormat.addInputPath(job, new Path("Input14/in"));
	//	FileOutputFormat.setOutputPath(job, new Path("output14"));
	//	Boolean status = job.waitForCompletion(true);
	//	return status ? 0 : 1;
		
		//job1.setJobName("Job1");
		job2.setJobName("Percentile calculation");
		
		job2.setJarByClass(HackDriver.class);
		job2.setMapperClass(PMapper.class);
		
		job2.setNumReduceTasks(0);
		//job2.setReducerClass(PReducer.class);
		FileInputFormat.addInputPath(job2, new Path("Input14/in"));
		FileOutputFormat.setOutputPath(job2, new Path("output15"));
		
		job2.submit();
		
		return 0;
	} */

	public static void main(String args[]) throws Exception {
		//int exitCode = ToolRunner.run(new Configuration(), new HackDriver(), args);
		//System.exit(exitCode);
		
		Job job1 = new Job();
		
		
		job1.setJarByClass(HackDriver.class);
		job1.setJobName("Nothing");
		job1.setMapperClass(HackMapper.class);
		job1.setCombinerClass(HackCombiner.class);
		job1.setReducerClass(HackReducer.class);
		//output format of the key emmited from reducer, this should match the reducer output key
		job1.setOutputKeyClass(Text.class);
		//output format of the key emitted from reducer, this should match the reducer output value format
		job1.setOutputValueClass(FloatWritable.class);
		// file i/o paths
		FileInputFormat.setInputPaths(job1, new Path("input"));
		FileOutputFormat.setOutputPath(job1, new Path("out1"));
		
		job1.waitForCompletion(true);
		
		Counters counters = job1.getCounters();
		
		
		System.out.println("Total number of counters : "
				+ counters.findCounter(COUNTERS.count).getValue());
		
		
		Configuration conf = new Configuration();
		conf.setLong("value", counters.findCounter(COUNTERS.count).getValue());
		Job job2 = new Job(conf);
		
		job2.setJobName("Aivnahs");
		job2.setJarByClass(HackDriver.class);
		job2.setMapperClass(PMapper.class);
		//job2.setNumReduceTasks(0);
		
		FileInputFormat.addInputPath(job2, new Path("input"));
		FileOutputFormat.setOutputPath(job2, new Path("output3"));
		
		job2.setMapperClass(PMapper.class);
		job2.setReducerClass(PReducer.class);
		
		job2.setMapOutputKeyClass(FloatWritable.class);
		job2.setMapOutputValueClass(IntWritable.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(FloatWritable.class);
		
		job2.submit();
	}
}
