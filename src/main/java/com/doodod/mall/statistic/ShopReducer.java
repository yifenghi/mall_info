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

import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;

public class ShopReducer extends
	Reducer<Text, BytesWritable, Text, BytesWritable> {
	enum JobCounter {
		NOT_IN_SHOP,
	}
	
	@Override
	public void reduce(Text key, Iterable<BytesWritable> values, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		
		for (BytesWritable val : values) {
			cb.mergeFrom(val.getBytes(), 0, val.getLength());
		}
		
		Map<Integer, Location.Builder> shopMap = new HashMap<Integer, Location.Builder>();
		for (Location.Builder lcb : cb.getLocationBuilderList()) {
			int shopId = lcb.getShopId();
		
			//out of frame & passenger : shopId = 0
			if (shopId == 0) {
				continue;
			}
			
			if (!shopMap.containsKey(shopId)) {
				shopMap.put(shopId, lcb);
			}
			else {
				Location.Builder locLast = shopMap.get(shopId);
				
				List<Long> timeList = new ArrayList<Long>();
				timeList.addAll(locLast.getTimeStampList());
				timeList.addAll(lcb.getTimeStampList());
				Collections.sort(timeList);
				
				locLast.clearTimeStamp().addAllTimeStamp(timeList);
			}
		}
		
		cb.clearLocation();
		List<Location> locationList = new ArrayList<Location>(); 
		Iterator<Integer> iter = shopMap.keySet().iterator();
		while (iter.hasNext()) {
			int shopId = iter.next();
			locationList.add(shopMap.get(shopId).build());
		}
		if (locationList.size() == 0) {
			context.getCounter(JobCounter.NOT_IN_SHOP).increment(1);
			return;
		}
		
		LocationComparator locCmp = new LocationComparator();
		Collections.sort(locationList, locCmp);
		cb.addAllLocation(locationList);
		
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
