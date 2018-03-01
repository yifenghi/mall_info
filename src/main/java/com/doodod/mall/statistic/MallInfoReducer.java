/**
 * @author yupeng.cao@palmaplus.com
 */
package com.doodod.mall.statistic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.doodod.mall.common.Coordinates;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;

public class MallInfoReducer extends 
	Reducer<Text, BytesWritable, Text, BytesWritable> {
	enum JobCounter {
		MULTI_LOACTIONS,
		COORDINATE_CONFLIT,
		REDUCE_OK,
	}
	
	@Override
	public void reduce(Text key, Iterable<BytesWritable> values, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		int counter = 0;
		for (BytesWritable val : values) {
			cb.mergeFrom(val.getBytes(), 0, val.getLength());
			counter ++;
		}
		
		if (counter > 1) {
			context.getCounter(JobCounter.MULTI_LOACTIONS).increment(1);
		}
		
		Map<Coordinates, Location.Builder> locationMap = new HashMap<Coordinates, Location.Builder>();
		for (Location.Builder loc : cb.getLocationBuilderList()) {
			Coordinates coordinates = new Coordinates(
					loc.getLocationX(), loc.getLocationY(), loc.getPlanarGraph());
			if (!locationMap.containsKey(coordinates)) {
				locationMap.put(coordinates, loc);
			}
			else {
				context.getCounter(JobCounter.COORDINATE_CONFLIT).increment(1);
				Location.Builder locLast = locationMap.get(coordinates);
				
				List<Long> timeList = new ArrayList<Long>();
				timeList.addAll(locLast.getTimeStampList());
				timeList.addAll(loc.getTimeStampList());

				Collections.sort(timeList);
				locLast.clearTimeStamp().addAllTimeStamp(timeList);
				
//				List<Long> clientTimeList = new ArrayList<Long>();
//				for (Long time : clientTimeList) {
//					clientTimeList.add(time);
//				}
//				Collections.sort(clientTimeList);
//				locLast.clearClientTime().addAllClientTime(clientTimeList);
			}	
		}
		
		cb.clearLocation();
		List<Location> locationList = new ArrayList<Location>(); 
		Iterator<Coordinates> iter = locationMap.keySet().iterator();
		while (iter.hasNext()) {
			Coordinates coordinates = iter.next();
			locationList.add(locationMap.get(coordinates).build());
		}
		LocationComparator locCmp = new LocationComparator();
		Collections.sort(locationList, locCmp);
		cb.addAllLocation(locationList);
						
		context.getCounter(JobCounter.REDUCE_OK).increment(1);
		context.write(key, new BytesWritable(cb.build().toByteArray()));
		
	}
	
	public class LocationComparator implements Comparator<Location> {
		public int compare(Location loc1, Location loc2) {
			int locSize1 = loc1.getTimeStampCount();
			int locSize2 = loc2.getTimeStampCount();
			int res = (loc1.getTimeStamp(locSize1 - 1)
						> loc2.getTimeStamp(locSize2 - 1)) ? -1 : 1;	
			return res;
		}
	}
	
}
