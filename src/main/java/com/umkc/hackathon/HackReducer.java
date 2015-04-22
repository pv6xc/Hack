package com.umkc.hackathon;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

enum ReduceCounter{
	count,
}


public class HackReducer extends
Reducer<Text, FloatWritable, Text, FloatWritable> {

	private float mapperCounter;
	private float sumCounter;
	private float squareSum;
	private float min;
	private float max;
	private float mean;
	private float sd;
	private float twentyFivePercentile;
	private float fiftyPercentile;
	private float seventyFivePercentile;
	private Integer tempSum=0;
	private Boolean visited=false;



	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		context.getCounter(ReduceCounter.count).increment(1);
	}


	/**
	 * Calculate the avg word count, i.e Sum of each word /total words
	 */
	@Override
	protected void reduce(Text key, Iterable<FloatWritable> value,
			Context context) throws IOException, InterruptedException {
		Text temp = new Text();
		FloatWritable floatTemp = new FloatWritable();

		String keyString = key.toString();

		long keyCount = context.getCounter(ReduceCounter.count).getValue();
		if(keyString.equalsIgnoreCase("-1"))
		{
			mapperCounter = value.iterator().next().get();
			temp.set("count");
			floatTemp.set(mapperCounter);
			twentyFivePercentile = (float) ((mapperCounter+1) * 0.25);
			fiftyPercentile= (float) ((mapperCounter+1) * 0.5);
			seventyFivePercentile = (float) ((mapperCounter+1) * 0.75);
			context.write(temp, floatTemp);
		}
		else if(keyString.equalsIgnoreCase("-2"))
		{
			sumCounter = value.iterator().next().get();
			temp.set("sum");
			floatTemp.set(sumCounter);
			context.write(temp, floatTemp);
		}
		else if(keyString.equalsIgnoreCase("-3"))
		{
			squareSum = value.iterator().next().get();
		}
		else if(keyString.equalsIgnoreCase("-4"))
		{
			min = value.iterator().next().get();
			temp.set("min");
			floatTemp.set(min);
			context.write(temp, floatTemp);
		}
		else if(keyString.equalsIgnoreCase("-5"))
		{
			max = value.iterator().next().get();
			temp.set("max");
			floatTemp.set(max);
			context.write(temp, floatTemp);
		}
		else if(keyString.equalsIgnoreCase("-6"))
		{
			mean = value.iterator().next().get();
			temp.set("mean");
			floatTemp.set(mean);
			context.write(temp, floatTemp);
		}
		else if(keyString.equalsIgnoreCase("-7"))
		{
			sd = value.iterator().next().get();
			temp.set("sd");
			floatTemp.set(sd);
			context.write(temp, floatTemp);
		}//emit 25th percentile value
		else if(keyCount == (long)twentyFivePercentile)
		{
			if(twentyFivePercentile == Math.ceil(twentyFivePercentile)){
				temp.set("25th percentile");
				floatTemp.set((Float.parseFloat(key.toString())+tempSum)/2);
				context.write(temp, floatTemp);
			}
			else 
			{
				if(tempSum!=0)
				{
					temp.set("25th percentile");
					floatTemp.set((Float.parseFloat(key.toString())+tempSum)/2);
					context.write(temp, floatTemp);
					tempSum=0;
				}else if(tempSum==0){

					tempSum = Integer.parseInt(key.toString());
					twentyFivePercentile++;
				}
			}
		}
		else if(keyCount == (long)fiftyPercentile)
		{
			if(fiftyPercentile== Math.ceil(fiftyPercentile))
			{
				temp.set("50th percentile");
				floatTemp.set(Float.parseFloat(key.toString()));
				context.write(temp, floatTemp);
			}
			else 
			{
				if(tempSum!=0)
				{
					temp.set("50th percentile");
					floatTemp.set((Float.parseFloat(key.toString())+tempSum)/2);
					context.write(temp, floatTemp);
					tempSum=0;
				}else if(tempSum==0){

					tempSum = Integer.parseInt(key.toString());
					fiftyPercentile++;
				}

			}
		}
		else if(keyCount == (long)seventyFivePercentile)
		{
			if(seventyFivePercentile == Math.ceil(seventyFivePercentile))
			{
				temp.set("75th percentile");
				floatTemp.set(Float.parseFloat(key.toString()));
				context.write(temp, floatTemp);
			}
			else 
			{
				if(tempSum!=0)
				{
					temp.set("75th percentile");
					floatTemp.set((Float.parseFloat(key.toString())+tempSum)/2);
					context.write(temp, floatTemp);
					tempSum=0;
				}else if(tempSum==0){
					tempSum = Integer.parseInt(key.toString());
					seventyFivePercentile++;
				}
			}
		}
		else
		{

			System.out.println(keyCount);
		}
		try
		{

			float tempVar =	Float.parseFloat(key.toString());
			if(tempVar>0)
				context.getCounter(ReduceCounter.count).increment(1);
		}
		catch(NumberFormatException e)
		{
			System.out.println("error parsing number : " + key.toString());
		}

	}

}
