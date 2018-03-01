package com.doodod.mall.analyse;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.doodod.mall.common.Common;

public class LocationTestReducer extends 
	Reducer<LongWritable, Text, LongWritable, Text> {

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int xError = 0;
		int xRight = 0;
		int yError = 0;
		int yRight = 0;
		
		for (Text val : values) {
			String arr[] = val.toString().split(Common.CTRL_A, -1);
			
			String arrX[] = arr[0].split(Common.CTRL_B, -1);
			xError += Integer.parseInt(arrX[0]);
			xRight += Integer.parseInt(arrX[1]);
			
			String arrY[] = arr[1].split(Common.CTRL_B, -1);
			yError += Integer.parseInt(arrY[0]);
			yRight += Integer.parseInt(arrY[1]);
		}
		
		Text ouVal = new Text(xError + Common.CTRL_B + xRight 
				+ Common.CTRL_A + yError + Common.CTRL_B + yRight);
		context.write(key, ouVal);
	}
	
	
}
