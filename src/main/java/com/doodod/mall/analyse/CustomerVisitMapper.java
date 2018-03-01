package com.doodod.mall.analyse;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Visit;

public class CustomerVisitMapper extends 
	Mapper<Text, BytesWritable, Text, Text> {
	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		
		StringBuffer sb = new StringBuffer();
		sb.append(1).append(Common.CTRL_A);
		for (Visit visit : cb.getUserVisitList()) {
			int times = visit.getVisitDateCount();
			int mallId = visit.getMallId();
			
			sb.append(mallId).append(Common.CTRL_A)
				.append(times).append(Common.CTRL_A);
		}
		sb.deleteCharAt(sb.length() - 1);
		
		context.write(key, new Text(sb.toString()));
	}

}
