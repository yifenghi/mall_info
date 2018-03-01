package com.doodod.mall.analyse;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;

public class MachineMapper extends 
	Mapper<Text, BytesWritable, Text, Text> {
	enum jobCounter {
		FILTER,
		PASSENGER,
		LOC_NUM_ZERO,
	}
	
	private static int DWELL_NUM_FILTER = 0;
	private static Set<Integer> FILTER_SET = new HashSet<Integer>();
	

	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		DWELL_NUM_FILTER = context.getConfiguration().getInt(
				Common.LOCATION_DWELL_FILTER, Common.DEFAULT_LOCATION_DWELL);
		String filters[] = context.getConfiguration().get(
				Common.CONF_FILTER_LIST).split(Common.COMMA, -1);
		for (String num : filters) {
			FILTER_SET.add(Integer.parseInt(num));
		}
		
	}
	
//  UserType {
//		  CUSTOMER  = 0;
//		  PASSENGER = 1;
//		  EMPLOYEE  = 2;
//		  MACHINE   = 3;
//	}
	
	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		
		
		if (!FILTER_SET.contains(cb.getUserType().getNumber())) {
			context.getCounter(jobCounter.FILTER).increment(1);
			return;
		}
		
		int locationNum = cb.getLocationCount();
		if (locationNum == 0) {
			context.getCounter(jobCounter.LOC_NUM_ZERO).increment(1);
			return;
		}
		String macBrand = Common.BRAND_UNKNOWN;
		if (cb.hasPhoneBrand()) {
			macBrand = new String(cb.getPhoneBrand().toByteArray());
		}
		Set<Long> floorSet = new HashSet<Long>();
		double sumDistance = 0;
		int dwell = 0;
		List<Double> distanceList = new ArrayList<Double>();
		for (int i = 0; i < cb.getLocationCount() - 1; i++) {
			Location current = cb.getLocation(i);
			Location next = cb.getLocation(i + 1);
			double distance = getDistance(current, next);
			sumDistance += distance;
			distanceList.add(distance);
			floorSet.add(current.getPlanarGraph());
			
			int timeCount = current.getTimeStampCount();
			dwell += (current.getTimeStamp(timeCount - 1) - current.getTimeStamp(0)) 
					/ Common.MINUTE_FORMATER;
		}
		Location last = cb.getLocation(cb.getLocationCount() - 1);
		int timeCount = last.getTimeStampCount();
		dwell += (last.getTimeStamp(timeCount - 1) - last.getTimeStamp(0)) 
				/ Common.MINUTE_FORMATER;
		floorSet.add(last.getPlanarGraph());
		
		if (dwell < DWELL_NUM_FILTER) {
			context.getCounter(jobCounter.PASSENGER).increment(1);
			return;
		}
		
		double avgDistance = 0;
		double varDistance = 0;
		
		if (cb.getLocationCount() > 1) {
			avgDistance = sumDistance / (cb.getLocationCount() - 1);
		}
		for (double distance : distanceList) {
			varDistance += Math.pow(
					distance - avgDistance , 2);
		}
		varDistance = Math.sqrt(varDistance);
		
		int floorNum = floorSet.size();
		double dwellPerPoint = 0;
		dwellPerPoint = dwell / locationNum;
		
		DecimalFormat df = new DecimalFormat(Common.NUM_FORNAT);
		Text outVal = new Text(floorNum + Common.CTRL_A
				+ df.format(avgDistance) + Common.CTRL_A
				+ df.format(varDistance) + Common.CTRL_A
				+ df.format(dwellPerPoint) + Common.CTRL_A
				+ locationNum + Common.CTRL_A 
				+ dwell + Common.CTRL_A
				+ macBrand);
		context.write(key, outVal);
		
	}
	
	private double getDistance(Location a, Location b) {
		return Math.sqrt(
				Math.pow(a.getLocationX() - b.getLocationX(), 2)
				+ Math.pow(a.getLocationY() - b.getLocationY(), 2));
	}
}
