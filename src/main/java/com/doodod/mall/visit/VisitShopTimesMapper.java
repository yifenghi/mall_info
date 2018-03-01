package com.doodod.mall.visit;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.UserType;
import com.doodod.mall.message.Mall.Visit;

public class VisitShopTimesMapper extends 
	Mapper<Text, BytesWritable, Text, Text> {
	enum JobCounter {
		NOT_CUSTOMER,
	}
	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb  = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		
		if (cb.getUserType() != UserType.CUSTOMER) {
			context.getCounter(JobCounter.NOT_CUSTOMER).increment(1);
			return;
		}
		
		for (Visit visit : cb.getUserVisitList()) {
			int shopId = visit.getShopId();
			//int shopCat = visit.getShopId();
			
			int times = visit.getVisitDateCount();
			int freq = 1;
			if (times > 1) {
				int days =  (int) (visit.getVisitDate(times - 1) 
						- visit.getVisitDate(0)) / (Common.MINUTE_FORMATER * 60 * 24);
				freq = days / times ;
			}
			
			Text outKey = new Text(Common.DWELL_TYPE_SHOP
					+ Common.CTRL_A + shopId);
			Text outVal = new Text(times + Common.CTRL_A + freq);
			
			context.write(outKey, outVal);
		}
	}

}
