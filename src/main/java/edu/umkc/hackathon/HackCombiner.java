package edu.umkc.hackathon;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author Prakash
 *
 */

public class HackCombiner extends
Reducer<Text, FloatWritable, Text, FloatWritable> {

	
	long count;
	long sum;
	long sumSquare;
	long min=Integer.MAX_VALUE;
	long max=Integer.MIN_VALUE;
	/**
	 * Calculate the avg word count, i.e Sum of each word /total words
	 */
	@Override
	protected void reduce(Text key, Iterable<FloatWritable> value,
			Context context) throws IOException, InterruptedException {
		float temp=0;
		Text tempText = new Text();
		FloatWritable floatTemp = new FloatWritable();
		if(key.toString().equals("count") || key.toString().equals("sum") || key.toString().equals("sumSquare")){
		for(FloatWritable var:value){
			temp += var.get();
		}
		floatTemp.set(temp);
		}
		else if(key.toString().equals("min")){
			for(FloatWritable var:value){
				if(var.get()<min)
					min=(long) var.get();
			}
			floatTemp.set(min);
		}
		else if(key.toString().equals("max")){
			for(FloatWritable var:value){
				if(var.get()>max)
					max=(long) var.get();
			}
			floatTemp.set(max);
		}
		if(key.toString().equalsIgnoreCase("count"))
			count=(long) temp;
		else if(key.toString().equalsIgnoreCase("sum"))
			sum=(long)temp;
		else if(key.toString().equalsIgnoreCase("sumSquare"))
			sumSquare=(long)temp;
		context.write(key, floatTemp);
		
	}

}
