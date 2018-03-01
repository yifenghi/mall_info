package com.doodod.mall.statistic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.common.TimeCluster;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;
import com.doodod.mall.message.Mall.UserType;


public class DwellShopMapper extends 
	Mapper<Text, BytesWritable, Text, Text> {
	enum JobCounter {
		CUST_MAC_CONFLICT,
		NOT_CUSTOMER,
		MALL_ID_ERROR,
	}

	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		
		String custMac = new String(cb.getPhoneMac().toByteArray());
		if (cb.getUserType() != UserType.CUSTOMER) {
			context.getCounter(JobCounter.NOT_CUSTOMER).increment(1);
			return;
		}
		
		int mallId = Common.getMallId(cb.build());
		if (mallId == 0) {
			context.getCounter(JobCounter.MALL_ID_ERROR).increment(1);
			return;
		}
		
		for (Location loc : cb.getLocationList()) {
			List<Long> timeList = new ArrayList<Long>(loc.getTimeStampList());
			long timeDwell = TimeCluster.getTimeDwell(
					timeList, Common.DEFAULT_DWELL_PASSENGER) / Common.MINUTE_FORMATER;
			if (timeDwell == 0) {
				timeDwell ++;
			}
			
			Text outKey = new Text(Common.DWELL_TYPE_SHOP + 
					Common.CTRL_A + String.valueOf(loc.getShopId()) +
					Common.CTRL_A + String.valueOf(mallId));
			Text outVal = new Text(custMac + Common.CTRL_A
					+ String.valueOf(timeDwell));
			context.write(outKey, outVal);
		}
		
	}
}
