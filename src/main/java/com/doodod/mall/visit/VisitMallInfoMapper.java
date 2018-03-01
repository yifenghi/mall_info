package com.doodod.mall.visit;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Visit;

public class VisitMallInfoMapper extends 
	Mapper<Text, BytesWritable, Text, Text> {
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
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		
		for (Visit vb : cb.getUserVisitList()) {			
			int mallId = vb.getMallId();
			int times = vb.getVisitDateCount();
			
			long latestDate = vb.getVisitDate(times - 1);
			if(latestDate != DATE_TIME) {
				continue;
			}
			
			int freq = 1;
			if (times > 1) {
				int days = (int) (vb.getVisitDate(times - 1) - vb.getVisitDate(0))
						/ (Common.MINUTE_FORMATER * 60 * 24);
				freq = days / times;
			}
			if (freq < 1) {
				freq = 1;
			}
			
			int newCustNum = 0;
			int oldCustNum = 0;
			
			if (times > 1) {
				oldCustNum ++;
			}
			else {
				newCustNum ++;
			} 
			
			Text outKey = new Text(Common.DWELL_TYPE_MALL
					+ Common.CTRL_A + mallId);
			Text outVal = new Text(times + Common.CTRL_A + freq
					+ Common.CTRL_A + newCustNum + Common.CTRL_A + oldCustNum);
			
			context.write(outKey, outVal);
			
		}
	}
}
