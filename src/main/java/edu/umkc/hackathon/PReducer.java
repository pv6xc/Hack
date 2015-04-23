package edu.umkc.hackathon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PReducer extends
		Reducer<FloatWritable, IntWritable, Text, FloatWritable> {
	
	
	ArrayList<Float> vals = new ArrayList<Float>();
	float count = 0; 

	@Override
	protected void setup(
			Reducer<FloatWritable,IntWritable,  Text, FloatWritable>.Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		String value = conf.get("value");
		System.out.println("inside reducer " + value);
		count = Float.parseFloat(value.toString());
	}

	@Override
	protected void reduce(
			FloatWritable key,
			Iterable<IntWritable> values,
			Reducer<FloatWritable,IntWritable, Text, FloatWritable>.Context context)
			throws IOException, InterruptedException {

		while(values.iterator().hasNext()) {
			vals.add(key.get());
			values.iterator().next();
		}
	}
	
	@Override
	protected void cleanup(
			Reducer<FloatWritable, IntWritable, Text, FloatWritable>.Context context)
			throws IOException, InterruptedException {
		
		System.out.println(count);
		
		// 25th percentile
		double tmp = (0.25*count);
		
		double lower = Math.floor(tmp);
		double top = Math.ceil(tmp);
				
		System.out.println(lower + " " +  top);
		
		double val1 = vals.get((int) lower -1);
		double val2 = vals.get((int) top -1);
		
		System.out.println("val1 " + val1 );
		System.out.println("val2 " + val2  );
		
		if(val1 != val2) {
			System.out.println("output" + (val1 + val2)/2);
		} else {
			System.out.println("output1" + val1);
		}
		
		
		
		// 50th percentile
				 tmp = (0.50*count);
				
				 lower = Math.floor(tmp);
				 top = Math.ceil(tmp);
						
				System.out.println(lower + " " +  top);
				
				 val1 = vals.get((int) lower -1);
				 val2 = vals.get((int) top -1);
				
				System.out.println("val1 " + val1 );
				System.out.println("val2 " + val2  );
				
				if(val1 != val2) {
					System.out.println("50th output" + (val1 + val2)/2);
				} else {
					System.out.println("50th output1" + val1);
				}
				
				
				
				
				// 75th percentile
				tmp = (0.75*count);
				
				 lower = Math.floor(tmp);
				top = Math.ceil(tmp);
						
				System.out.println(lower + " " +  top);
				
				 val1 = vals.get((int) lower -1);
				 val2 = vals.get((int) top -1);
				
				System.out.println("val1 " + val1 );
				System.out.println("val2 " + val2  );

				if(val1 != val2) {
					System.out.println(" 75 th output" + (val1 + val2)/2);
				} else {
					System.out.println(" 75th output1" + val1);
				}
	}
}
