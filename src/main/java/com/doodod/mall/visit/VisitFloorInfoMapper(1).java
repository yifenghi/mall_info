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

public class VisitFloorInfoMapper extends 
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
		Customer.Builder cb  = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		
		for (Visit visit : cb.getUserVisitList()) {
			long floorId = visit.getPlanarGraph();
			int times = visit.getVisitDateCount();
			
			long latestDate = visit.getVisitDate(times - 1);
			if(latestDate != DATE_TIME) {
				continue;
			}
			
			int freq = 0;
			if (times > 1) {
				int days =  (int) (visit.getVisitDate(times - 1) - visit.getVisitDate(0)) 
						/ (Common.MINUTE_FORMATER * 60 * 24);
				freq = days / (times - 1) ;
			}
			
			int newCustNum = 0;
			int oldCustNum = 0;
			if (times > 1) {
				oldCustNum ++;
			}
			else {
				newCustNum ++;
			}
			
			Text outKey = new Text(Common.DWELL_TYPE_SHOP
					+ Common.CTRL_A + floorId);
			Text outVal = new Text(times + Common.CTRL_A + freq
					+ Common.CTRL_A + newCustNum + Common.CTRL_A + oldCustNum);
			
			context.write(outKey, outVal);
		}
	}
}
