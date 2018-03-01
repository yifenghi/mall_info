package com.doodod.mall.statistic;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;
import com.doodod.mall.message.Mall.UserType;

public class DwellMallMapper extends 
	Mapper<Text, BytesWritable, Text, Text>{
	enum JobCounter {
		NOT_CUSTOMER,
	}
	private static long MALL_ID = Common.DEFAULT_MALL_ID;
	
	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		
		if (cb.getUserType() != UserType.CUSTOMER) {
			context.getCounter(JobCounter.NOT_CUSTOMER).increment(1);
			return;
		}
		
		long dwell = 0;
		Set<Long> floorSet = new HashSet<Long>();
		for (Location loc : cb.getLocationList()) {
			floorSet.add(loc.getPlanarGraph());
			
			int listSize = loc.getTimeStampCount();
			dwell += (loc.getTimeStamp(listSize - 1) - loc.getTimeStamp(0))
					/ Common.MINUTE_FORMATER;
		}
		
		int floorSize = floorSet.size();
		if (floorSize < 1) {
			floorSize = 1;
		}
		Text outKey = new Text(String.valueOf(MALL_ID));
		Text outVal = new Text(floorSize + Common.CTRL_A + dwell);

		context.write(outKey, outVal);
	}
}
