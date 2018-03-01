package com.doodod.mall.statistic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.common.TimeCluster;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;
import com.doodod.mall.message.Mall.UserType;


public class DwellFloorMapper extends 
	Mapper<Text, BytesWritable, Text, Text> {
	enum JobCounter {
		CUST_MAC_CONFLICT,
		NOT_CUSTOMER,
		MALL_ID_ERROR,
	}
	//private Map<String, Map<Long, Long>> floorMap = new HashMap<String, Map<Long, Long>>();
	
	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		
		if (cb.getUserType() != UserType.CUSTOMER) {
			context.getCounter(JobCounter.NOT_CUSTOMER).increment(1);
			return;
		}
		
		long mallId = Common.getMallId(cb.build());
		if (mallId == 0) {
			context.getCounter(JobCounter.MALL_ID_ERROR).increment(1);
			return;
		}
		
		Map<Long, Location.Builder> floorBuilderMap = new HashMap<Long, Location.Builder>(); 
		String custMac = new String(cb.getPhoneMac().toByteArray());
		for (Location loc : cb.getLocationList()) {
			long floorId = loc.getPlanarGraph();
			if (floorBuilderMap.containsKey(floorId)) {
				Location.Builder lcb = floorBuilderMap.get(floorId);
				for (long time : loc.getTimeStampList()) {
					lcb.addTimeStamp(time);
				}
			}
			else {
				Location.Builder lcb = Location.newBuilder();
				lcb.mergeFrom(loc).clearLocationX().clearLocationY();
				floorBuilderMap.put(floorId, lcb);
			}
		}
		
		//Map<Long, Long> dwellMap = new HashMap<Long, Long>();
		Iterator<Long> floorIter = floorBuilderMap.keySet().iterator();
		while (floorIter.hasNext()) {
			long floorId = floorIter.next();
			Location.Builder lcb = floorBuilderMap.get(floorId);
			List<Long> timeList = new ArrayList<Long>(lcb.getTimeStampList());
			//Collections.sort(timeList);
			
//			long timeDwell = timeList.get(timeList.size() -1)
//					- timeList.get(0) + 1;
			long timeDwell = TimeCluster.getTimeDwell(timeList, Common.DEFAULT_DWELL_PASSENGER)
					/ Common.MINUTE_FORMATER;
			
			if (timeDwell == 0) {
				timeDwell ++;
			}
			
			//dwellMap.put(floorId, timeDwell);
			Text outKey = new Text(Common.DWELL_TYPE_FLOOR
					+ Common.CTRL_A + String.valueOf(floorId));
			Text outVal = new Text(custMac
					+ Common.CTRL_A + String.valueOf(timeDwell));
			context.write(outKey, outVal);
		}	
	}
}
