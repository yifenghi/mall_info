package com.doodod.mall.statistic;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.common.TimeCluster;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;
import com.doodod.mall.message.Mall.UserType;

public class DwellMallMapper extends 
	Mapper<Text, BytesWritable, Text, Text>{
	enum JobCounter {
		NOT_CUSTOMER,
		MALL_ID_ERROR,
	}
	
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
		
		Set<Long> floorSet = new HashSet<Long>();
		for (Location loc : cb.getLocationList()) {
			floorSet.add(loc.getPlanarGraph());
		}
		
		long dwell = TimeCluster.getTimeDwell(cb.build(), Common.DEFAULT_DWELL_PASSENGER)
				/ Common.MINUTE_FORMATER;
		
		int floorSize = floorSet.size();
		if (floorSize < 1) {
			floorSize = 1;
		}
		Text outKey = new Text(String.valueOf(mallId));
		Text outVal = new Text(floorSize + Common.CTRL_A + dwell);

		context.write(outKey, outVal);
	}
}
