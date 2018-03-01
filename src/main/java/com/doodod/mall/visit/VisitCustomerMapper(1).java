package com.doodod.mall.visit;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.UserType;

public class VisitCustomerMapper extends
	Mapper<Text, BytesWritable, LongWritable, Text> {
	enum JobCounter {
		MALL_ID_ERROR,
	}
	
	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		
		int mallId = Common.getMallId(cb.build());
		if (mallId == 0) {
			context.getCounter(JobCounter.MALL_ID_ERROR).increment(1);
		}
		
		int customerNum = 0;
		int employeeNum = 0;
		int machineNum = 0;
		int passengerNum = 0;
		
		if (!cb.hasUserType()) {
			customerNum ++;
		}
		else {
			switch (cb.getUserType().getNumber()) {
			case UserType.CUSTOMER_VALUE:
				customerNum ++;
				break;
			case UserType.EMPLOYEE_VALUE:
				employeeNum ++;
				break;
			case UserType.MACHINE_VALUE:
				machineNum ++;
				break;
			case UserType.PASSENGER_VALUE:
				passengerNum ++;
				break;
			default:
				break;
			}
		}
		
		LongWritable outKey = new LongWritable(mallId);
		Text outVal = new Text(customerNum + Common.CTRL_A 
				+ employeeNum + Common.CTRL_A 
				+ machineNum + Common.CTRL_A
				+ passengerNum + Common.CTRL_A
				+ 1);
		
		context.write(outKey, outVal);
	}
}
