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

public class FlowMallShopMapper extends 
	Mapper<Text, BytesWritable, Text, Text> {
	enum JobCounter {
		NOT_CUSTOMER,
	}
	
	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		//TODO get mall id from shop id
		int mallId = Common.DEFAULT_MALL_ID;
		
		if (cb.getUserType() != UserType.CUSTOMER) {
			context.getCounter(JobCounter.NOT_CUSTOMER).increment(1);
			return;
		}
		
		Set<Integer> zoneSet = new HashSet<Integer>();
		for(Location loc : cb.getLocationList()) {
			zoneSet.add(loc.getShopCat());
		}
		int shopNum = cb.getLocationCount();
		int zoneNum = zoneSet.size();
		Text outKey = new Text(String.valueOf(mallId));
		Text outVal = new Text(shopNum + Common.CTRL_A + zoneNum);
		context.write(outKey, outVal);
	}
}
