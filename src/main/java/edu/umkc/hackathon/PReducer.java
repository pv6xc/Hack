package edu.umkc.hackathon;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PReducer extends
Reducer<FloatWritable, IntWritable, Text, FloatWritable> {


	private float twentyFivePercentile;
	private float fiftyPercentile;
	private float seventyFivePercentile;
	private float tempSum=0;
	ArrayList<Float> vals = new ArrayList<Float>();
	float count = 0; 
	long keyCount=1;

	@Override
	protected void setup(
			Reducer<FloatWritable,IntWritable,  Text, FloatWritable>.Context context)
					throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		String value = conf.get("value");
		count = Float.parseFloat(value.toString());
		twentyFivePercentile = (float) ((count+1) * 0.25);
		fiftyPercentile= (float) ((count+1) * 0.5);
		seventyFivePercentile = (float) ((count+1) * 0.75);
	}

	@Override
	protected void reduce(
			FloatWritable key,
			Iterable<IntWritable> values,
			Reducer<FloatWritable,IntWritable, Text, FloatWritable>.Context context)
					throws IOException, InterruptedException {
		Text temp = new Text();
		FloatWritable floatTemp = new FloatWritable();
		for (IntWritable var:values)
		{
			if(keyCount == (long)twentyFivePercentile)
			{
				if(twentyFivePercentile == Math.ceil(twentyFivePercentile)){
					temp.set("25th percentile");
					floatTemp.set((key.get()+tempSum)/2);
					context.write(temp, floatTemp);
					keyCount++;
				}
				else 
				{
					if(tempSum!=0)
					{
						temp.set("25th percentile");
						floatTemp.set((key.get()+tempSum)/2);
						context.write(temp, floatTemp);
						keyCount++;
						tempSum=0;
					}else if(tempSum==0){
						tempSum = key.get();
						twentyFivePercentile++;
						keyCount++;
					}
				}
			}
			else if (keyCount == (long)fiftyPercentile)
			{
				if(fiftyPercentile== Math.ceil(fiftyPercentile))
				{
					temp.set("50th percentile");
					floatTemp.set(key.get());
					context.write(temp, floatTemp);
					keyCount++;
				}
				else if(tempSum!=0)
				{
					temp.set("50th percentile");
					floatTemp.set((key.get()+tempSum)/2);
					context.write(temp, floatTemp);
					keyCount++;
					tempSum=0;
				}else if(tempSum==0){

					tempSum = key.get();
					fiftyPercentile++;
					keyCount++;
				}

			}
			else if (keyCount == (long)seventyFivePercentile)
			{
				if(seventyFivePercentile == Math.ceil(seventyFivePercentile))
				{
					temp.set("75th percentile");
					floatTemp.set(key.get());
					context.write(temp, floatTemp);
					keyCount++;
				}
				else 
				{
					if(tempSum!=0)
					{
						temp.set("75th percentile");
						floatTemp.set((key.get()+tempSum)/2);
						context.write(temp, floatTemp);
						keyCount++;
						tempSum=0;
					}else if(tempSum==0){
						tempSum = key.get();
						seventyFivePercentile++;
						keyCount++;
					}
				}
			}

		}//for loop on values 
	}
}
