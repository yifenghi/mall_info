package com.doodod.mall.analyse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.common.TimeCluster;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;
import com.doodod.mall.message.Mall.UserType;

public class CustomerAtFloorMapper extends 
	Mapper<Text, BytesWritable, Text, Text> {
	private static List<Long> FLOOR_ID_LIST = new ArrayList<Long>();;
	
	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		String floors = context.getConfiguration().get(
				Common.ANALYSE_CUSTOMER_FLOOR);
		String arr[] = floors.split(Common.COMMA, -1);
		for (String str : arr) {
			FLOOR_ID_LIST.add(Long.parseLong(str));
		}
	}
	
	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		
		if (cb.getUserType() != UserType.CUSTOMER) {
			context.getCounter("ANALYSE", "NOT_CUSTOMER").increment(1);
			return;
		}
		
		Set<Long> floorSet = new HashSet<Long>();
		for (Location loc : cb.getLocationList()) {
			floorSet.add(loc.getPlanarGraph());
		}
		
		boolean hasFloorId = false;
		for (Long floor : FLOOR_ID_LIST) {
			if (floorSet.contains(floor)) {
				hasFloorId = true;
			}
		}
		
		if (!hasFloorId) {
			context.getCounter("ANALYSE", "NOT_AT_FLOOR").increment(1);
			return;
		}
		
		Map<Long, List<Long>> dwellMap = new HashMap<Long, List<Long>>();
		Map<Long, Integer> locationsMap = new HashMap<Long, Integer>();
		for (Location loc : cb.getLocationList()) {
			long floorId = loc.getPlanarGraph();
			if (dwellMap.containsKey(floorId)) {
				List<Long> timeList = dwellMap.get(floorId);
				for (Long time : loc.getTimeStampList()) {
					timeList.add(time);
				}
				locationsMap.put(floorId, locationsMap.get(floorId) + 1);
			}
			else {
				List<Long> timeList = new ArrayList<Long>();
				timeList.addAll(loc.getTimeStampList());
				dwellMap.put(floorId, timeList);
				locationsMap.put(floorId, 1);
			}
		}
		
		StringBuffer customerInfo = new StringBuffer();
		Iterator<Long> iter = dwellMap.keySet().iterator();
		
		long dwellSum = 0;
		long dwellFloor = 0;
		while (iter.hasNext()) {
			long floorId = iter.next();
			long dwell = TimeCluster.getTimeDwell(
					dwellMap.get(floorId), Common.DEFAULT_DWELL_PASSENGER) 
					/ Common.MINUTE_FORMATER;
			long locations = locationsMap.get(floorId);
			customerInfo.append(floorId).append(Common.CTRL_B)
				.append(dwell).append(Common.CTRL_B)
				.append(locations).append(Common.CTRL_A);
			dwellSum += dwell;
			if (FLOOR_ID_LIST.contains(floorId)) {
				dwellFloor += dwell;
			}
		}
		
		if (dwellSum == 0) {
			dwellSum ++;
		}
		
		double percent = (dwellFloor * 1.0) / dwellSum;
		customerInfo.insert(0, 0 + Common.CTRL_A + String.format("%.2f", percent) + Common.CTRL_A);
		customerInfo.append(new String(cb.getPhoneBrand().toByteArray()));
		//customerInfo.deleteCharAt(customerInfo.length() - 1);
		context.write(key, new Text(customerInfo.toString()));
	}
}
