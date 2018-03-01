/**
 * @author yupeng.cao@palmaplus.com
 */
package com.doodod.mall.statistic;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;

public class MergeShopReducer extends 
	Reducer<Text, BytesWritable, Text, BytesWritable> {	
	enum JobCounter {
		TOTAL_REDUCE_OK,
		PART_REDUCE_OK,
		INVALID_RECORD,
		TOATL_IS_EMPTY,
		PART_IS_EMPTY,
		PART_COORDINATE_CONFLIT,
		PART_NEW,
		PART_IN_OLD,
	}
	
	@Override
	public void reduce(Text key, Iterable<BytesWritable> values, Context context)
			throws IOException, InterruptedException {
		Customer.Builder total = Customer.newBuilder();
		Customer.Builder part = Customer.newBuilder();

		for (BytesWritable val : values) {
			byte[] arr = val.getBytes();
			if (arr[0] == Common.MERGE_TAG_T) {
				total.clear().mergeFrom(val.getBytes(), 1, val.getLength() - 1);
				context.getCounter(JobCounter.TOTAL_REDUCE_OK).increment(1);
			} else if (arr[0] == Common.MERGE_TAG_P) {
				part.clear().mergeFrom(val.getBytes(), 1, val.getLength() - 1);
				context.getCounter(JobCounter.PART_REDUCE_OK).increment(1);
			} else {
				context.getCounter(JobCounter.INVALID_RECORD).increment(1);
			}
		}
		

		if (total.getLocationCount() == 0) {
			context.getCounter(JobCounter.TOATL_IS_EMPTY);
			total.clear().mergeFrom(part.build());
		}
		else {
			if (part.getLocationCount() == 0) {
				context.getCounter(JobCounter.PART_IS_EMPTY);
			}
			else {
				//build map with part
				Map<Integer, Location> locationMap = new HashMap<Integer, Location>();
				Set<Integer> shopInTotal = new HashSet<Integer>();
				
				for (Location loc : part.getLocationList()) {
					int shopId = loc.getShopId();
					if (!locationMap.containsKey(shopId)) {
						locationMap.put(shopId, loc);
					}
					else {
						context.getCounter(JobCounter.PART_COORDINATE_CONFLIT).increment(1);
					}	
				}
				
				//update old locations
				for (Location.Builder loc : total.getLocationBuilderList()) {
					int shopId = loc.getShopId();
					if (locationMap.containsKey(shopId)) {
						Location newLoc = locationMap.get(shopId);
						loc.addAllTimeStamp(newLoc.getTimeStampList());
						//loc.addAllClientTime(newLoc.getClientTimeList());	
						shopInTotal.add(shopId);
						context.getCounter(JobCounter.PART_IN_OLD).increment(1);
					}
				}
				
				//add new locations to total
				Iterator<Integer> iter = locationMap.keySet().iterator();
				while (iter.hasNext()) {
					int id= iter.next();
					if (shopInTotal.contains(id)) {
						continue;
					}
					total.addLocation(locationMap.get(id));
					context.getCounter(JobCounter.PART_NEW).increment(1);
				}
				
			}
		}
		
	}

}
