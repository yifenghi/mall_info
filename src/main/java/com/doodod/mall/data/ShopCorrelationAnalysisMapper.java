package com.doodod.mall.data;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;
import com.doodod.mall.message.Mall.UserType;
import com.doodod.mall.message.Mall.Visit;

public class ShopCorrelationAnalysisMapper extends
		Mapper<Text, BytesWritable, Text, LongWritable> {
	enum JobCounter {
		NOT_CUSTOMER,
		SHOP_OK
	}
	@Override
	protected void map(Text key, BytesWritable value,Context context)
			throws IOException, InterruptedException {

		Customer.Builder cb = Customer.newBuilder();
		cb.mergeFrom(value.getBytes(),0,value.getLength());
		if (cb.getUserType() != UserType.CUSTOMER) {
			context.getCounter(JobCounter.NOT_CUSTOMER).increment(1);	
			return;
		}
		
		for(Location.Builder lb : cb.getLocationBuilderList()){
			context.getCounter(JobCounter.SHOP_OK).increment(1);	
			String name=new String(lb.getShopName().toByteArray());
			context.write(new Text(lb.getShopId()+","+name), new LongWritable(1));
			
		}
		
		
	}

}
