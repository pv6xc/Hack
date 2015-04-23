package edu.umkc.hackathon;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.eclipse.jdt.core.dom.ThisExpression;

/**
 * 
 * @author Prakash
 *
 */
public class HackMapper   extends
Mapper<LongWritable, Text, Text, FloatWritable> {

	long counter=0;
	long sumCount=0;
	long squareSumCount=0;
	long minValue=Integer.MAX_VALUE;
	long maxValue=Integer.MIN_VALUE;
	/**
	 * Emit number as key and 1
	 */
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {

		Text number = new Text();
		FloatWritable one = new FloatWritable(1);
		Integer line =  Integer.parseInt(value.toString());
		//count the number of integers
		//will be incremented by one for each number read from the input files
		counter++;
		//count the sum of all the integers
		//will add the value of each number read to a global counter "sum"
		sumCount+=line;
		//count the sum of square of the numbers for SD.
		//will add the value of each number read to a global counter "number squared sum"
		squareSumCount+=line*line;
		//finding min  
		//compare with current number 
		if(line<minValue)
		{
			//update the min value
			minValue=line;
		}
		//finding max get max
		//compare with current number 
		if(line>maxValue)
		{
			//update the max value
			maxValue=line;
		}
		//update the Text key value with the integer received
		number.set(line.toString());
		//write each number read from the file as key.
		//This would enable the shuffle and sort phase 
		//to take care of the sorting of the data for percentile calculation
	}

	/**
	 * Method will run only once after the map phase is completed by all the mappers in all the nodes.
	 * This method would write the global counters to context, such that reducer can read these counters
	 * for percentile calculation and again to write to a file.
	 * Required calculation are done for mean and standard deviation and the data is later written to context
	 */
	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
		
		context.write(new Text("count"), new FloatWritable(this.counter));
		context.write(new Text("sum"), new FloatWritable(this.sumCount));
		context.write(new Text("sumSquare"), new FloatWritable(this.squareSumCount));
		context.write(new Text("min"), new FloatWritable(this.minValue));
		context.write(new Text("max"), new FloatWritable(this.maxValue));
	}



}
