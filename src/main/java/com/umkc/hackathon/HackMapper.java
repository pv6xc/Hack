package com.umkc.hackathon;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class HackMapper   extends
Mapper<LongWritable, Text, Text, FloatWritable> {

	Float mean  ;
	
static enum WordsCount{
count,
sum,
squareSum,
min,
max,

}
/**
* Emit word and 1
*/
@Override
protected void map(LongWritable key, Text value,Context context)
	throws IOException, InterruptedException {
	
	Text number = new Text();
	FloatWritable one = new FloatWritable(1);
	Integer line =  Integer.parseInt(value.toString());
	//count the number of integers
	context.getCounter(WordsCount.count).increment(1);
	//count the sum of all the integers
	context.getCounter(WordsCount.sum).increment(line);
	//count the sum of square of the numbers for SD.
	context.getCounter(WordsCount.squareSum).increment(line*line);
	//finding min
		//get min
		long min =context.getCounter(WordsCount.min).getValue();
		//compare with current number 
		if(line<min)
		{
			//update the min value
			context.getCounter(WordsCount.min).setValue(line);
		}else if(min==0)
		{
			context.getCounter(WordsCount.min).setValue(line);
		}
	//finding max
		//get max
		long max =context.getCounter(WordsCount.max).getValue(); 
		//compare with current number 
		if(line>max)
		{
			//update the max value
			context.getCounter(WordsCount.max).setValue(line);
		}
		number.set(line.toString());
		context.write(number,one);
}

/**
 * 
 */
@Override
protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
		throws IOException, InterruptedException {
	super.cleanup(context);
	Text temp = new Text();
	FloatWritable counter = new FloatWritable();
	long count= context.getCounter(WordsCount.count).getValue();
	long sumValue= context.getCounter(WordsCount.sum).getValue();
	long sumSquare = context.getCounter(WordsCount.squareSum).getValue();
	//set counter for reducer to read
	counter.set(count);
	temp.set("-1");
	context.write(temp,counter);
	//set sum value for reducer to read
	counter.set(sumValue);
	temp.set("-2");
	context.write(temp,counter);
	//set sum square value for reducer to read
	counter.set(context.getCounter(WordsCount.squareSum).getValue());
	temp.set("-3");
	context.write(temp,counter);
	//set min value for reducer to read
	counter.set(context.getCounter(WordsCount.min).getValue());
	temp.set("-4");
	context.write(temp,counter);
	//set max value for reducer to read
	counter.set(context.getCounter(WordsCount.max).getValue());
	temp.set("-5");
	context.write(temp,counter);
	//set mean value for reducer to read
	counter.set(sumValue/count);
	temp.set("-6");
	context.write(temp,counter);
	//calculating variance
	///std dev = sqrt [(B - A^2/N)/N]
	//A is the sum of the data values;
	//B is the sum of the squared data values;
	//N is the number of data values.
	float variance = (float) (sumSquare - (Math.pow(sumValue,2)/count));
	float sd = (float) Math.sqrt(Math.abs(variance/(count-1)));
	counter.set(sd);
	temp.set("-7");
	context.write(temp,counter);
	
}



}
