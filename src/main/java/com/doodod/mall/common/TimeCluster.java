package com.doodod.mall.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;


public class TimeCluster {
	public static List<Long> getTimeList(Customer customer) {
		List<Long> timeList = new ArrayList<Long>();		
		for (Location loc : customer.getLocationList()) {
			timeList.addAll(loc.getTimeStampList());
		}	
		return timeList;
	}
	
	public static List<List<Long>> getTimeZone(List<Long> timeList, long timeSplit) {
		List<Long> timeListNew = new ArrayList<Long>();
		timeListNew.addAll(timeList);
		
		List<List<Long>> zoneList = new ArrayList<List<Long>>();	
		if (timeListNew.size() == 1) {
			zoneList.add(timeListNew);
			return zoneList;
		}
		
		Collections.sort(timeListNew);
		int size = timeListNew.size();
		zoneList.add(new ArrayList<Long>());
		for (int i = 0; i < size - 1; i++) {
			long zone = timeListNew.get(i + 1) - timeListNew.get(i);
			zoneList.get(zoneList.size() - 1).add(timeListNew.get(i));

			if (zone > timeSplit) {
				zoneList.add(new ArrayList<Long>());
			}
			
			if (i == size - 2) {
				zoneList.get(zoneList.size() - 1).add(timeListNew.get(i + 1));
			}
		}
		
		return zoneList;
	}
	
	public static long getTimeDwell(Customer customer, long timeSplit) {
		List<Long> timeList = getTimeList(customer);
		return getTimeDwell(timeList, timeSplit);
	}
	
	public static long getTimeDwell(List<Long> timeList, long timeSplit) {
		List<List<Long>> zoneList = getTimeZone(timeList, timeSplit);
		long timeDwell = 0;
		for (List<Long> times : zoneList) {
			if (times.size() == 1) {
				//one point means 2 second
				timeDwell += 1 * Common.MINUTE_FORMATER / 30;
			}
			else {
				timeDwell += times.get(times.size() - 1)
						- times.get(0);
			}
		}
		return timeDwell;
	}
	
	
	public static long getMinTimeStamp(Customer customer) {
		long timeStamp = Long.MAX_VALUE;
		for (Location loc : customer.getLocationList()) {
			for (long time : loc.getTimeStampList()) {
				if (timeStamp > time) {
					timeStamp = time;
				}
			}
		}
		return timeStamp;	
	}
	
	public static long getMaxTimeStamp(Customer customer) {
		long timeStamp = 0;
		for (Location loc : customer.getLocationList()) {
			for (long time : loc.getTimeStampList()) {
				if (timeStamp < time) {
					timeStamp = time;
				}
			}
		}
		return timeStamp;	
	}
	
	public static void main(String[] args) {
		Location.Builder lcb0 = Location.newBuilder();
		lcb0.addTimeStamp(1417476900806L);
		lcb0.addTimeStamp(1417476980932L);
		
		Location.Builder lcb1 = Location.newBuilder();
		lcb1.addTimeStamp(1417476900700L);
		lcb1.addTimeStamp(1417476980940L);
		lcb1.addTimeStamp(1417476981941L);

		
		Customer.Builder cb = Customer.newBuilder();
		cb.addLocation(lcb0.build());
		cb.addLocation(lcb1.build());
		
		System.out.println(getTimeDwell(cb.build(), 600000) / Common.MINUTE_FORMATER);
		
		System.out.println(getMaxTimeStamp(cb.build()));

	}

}
