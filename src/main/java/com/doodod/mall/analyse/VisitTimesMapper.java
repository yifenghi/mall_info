package com.doodod.mall.analyse;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Visit;

public class VisitTimesMapper extends 
	Mapper<Text, BytesWritable, Text, Text> {
	enum JobCounter {
		HAS_NO_TYPE,
		MACHINE,
		EMPLOYEE,
	}
	
	private static long DATE_TIME = 0;
	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		
		try {
			DATE_TIME = timeFormat.parse(
					context.getConfiguration().get(Common.MALL_SYSTEM_TODAY)).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void map(Text key, BytesWritable value,Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		
		int typeNum = 5;
		if (!cb.hasUserType()) {
			context.getCounter(JobCounter.HAS_NO_TYPE).increment(1);
		}
		else {
			typeNum = cb.getUserType().getNumber();
		}
		
		
		for (Visit visit : cb.getUserVisitList()) {
			int times = visit.getVisitDateCount();
			int inToday = 1;
			
			long latestDate = visit.getVisitDate(times - 1);
			if(latestDate != DATE_TIME) {
				inToday = 0;
			}
			
			
			
			Text outKey = new Text(key.toString() + "\t"
					+ visit.getMallId());
			Text outVal = new Text(times + "\t" + inToday + "\t" + typeNum);
			
			context.write(outKey, outVal);
		}
	}

}
