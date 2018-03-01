package com.doodod.mall.analyse;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.doodod.mall.common.Common;

public class MachineCalculatorReducer extends 
	Reducer<Text, Text, Text, Text> {
	public static double DWELL_PER_POINT_FILTER = 0;
	public static int VISIT_TIMES_FILTER = 0;
	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		DWELL_PER_POINT_FILTER = context.getConfiguration().getDouble(
				Common.DWELL_PER_LOCATION_FILTER, Common.DEFAULT_DWELL_PER_LOCATION);
		VISIT_TIMES_FILTER = context.getConfiguration().getInt(
				Common.VISIT_TIMES_FILTER, Common.DEFAULT_VISIT_TIMES);
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int counter = 0;
		int sumFloor = 0;
		int sumLocation = 0;
		int sumDwell = 0;
		double sumDistance = 0;
		double sumVar = 0;
		String brand = Common.BRAND_UNKNOWN;
		
		for (Text value : values) {
			String arr[] = value.toString().split(Common.CTRL_A, -1);
			
			int floorNum = Integer.parseInt(arr[0]);
			double avgDistance = Double.parseDouble(arr[1]);
			double varDistance = Double.parseDouble(arr[2]);
			int locationNum = Integer.parseInt(arr[4]);
			int dwell = Integer.parseInt(arr[5]);
			brand = arr[6];
			
			sumFloor += floorNum;
			sumLocation += locationNum;
			sumDwell += dwell;
			
			sumDistance += avgDistance * (locationNum - 1);
			sumVar += Math.pow(varDistance, 2);
			counter ++;
		}
		
		int avgFloor = 0;
		int avgLocation = 0;
		int avgDwell = 0;
		double avgDistance = 0;
		double avgVar = 0;

		if (counter == 0) {
			return;
		}
		avgFloor = sumFloor / counter;
		avgLocation = sumLocation / counter;
		avgDwell = sumDwell / counter;
		double dwellPerLocation = avgDwell / avgLocation;
		
		if (sumLocation == 1) {
			avgDistance = sumDistance;
		}
		else {
			avgDistance = sumDistance / (sumLocation - 1);
		}
		avgVar = Math.sqrt(sumVar);

		String tag = "";
		if (dwellPerLocation >= DWELL_PER_POINT_FILTER) {
			tag = "MACHINE";
		}
		else if (counter >= VISIT_TIMES_FILTER) {
			tag = "EMPLOYEE";
		}
		else {
			tag = "NORMAL";
		}
		
		DecimalFormat df = new DecimalFormat(Common.NUM_FORNAT);
		Text outVal = new Text(
				avgFloor + Common.CTRL_A +
				df.format(avgDistance) + Common.CTRL_A + 
				df.format(avgVar) + Common.CTRL_A +
				df.format(dwellPerLocation) + Common.CTRL_A +
				avgLocation + Common.CTRL_A +
				avgDwell + Common.CTRL_A +
				counter + Common.CTRL_A + 
				tag + Common.CTRL_A +
				brand);
		
		context.write(key, outVal);
	}
}
