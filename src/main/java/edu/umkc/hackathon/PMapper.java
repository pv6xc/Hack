package edu.umkc.hackathon;

import java.io.IOException;

import org.apache.commons.io.output.NullWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PMapper extends
		Mapper<LongWritable, Text,  FloatWritable, IntWritable> {
	
	
	@Override
	protected void setup(
			Mapper<LongWritable, Text, FloatWritable , IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		String value = conf.get("value");
		System.out.println("inside mapper " + value);
	}
	
	@Override
	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, FloatWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		context.write(new FloatWritable(Float.parseFloat(value.toString())), new IntWritable(1));
	}
}
