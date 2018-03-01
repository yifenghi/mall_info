package com.doodod.mall.data;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ShopCorrelationAnalysisReducer extends
		Reducer<Text, LongWritable, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,Context context)
			throws IOException, InterruptedException {

		long sum=0;
		
		for(LongWritable val:values){
			sum+=val.get();
		}

		context.write(key, new Text(sum+""));
	}

}
