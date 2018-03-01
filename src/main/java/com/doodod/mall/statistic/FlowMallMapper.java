package com.doodod.mall.statistic;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Flow;
import com.doodod.mall.message.Mall.Location;
import com.doodod.mall.message.Mall.UserType;

public class FlowMallMapper extends
	Mapper<Text, BytesWritable, Text, BytesWritable> {
	enum JobCounter {
		MAP_OK,
		MAP_Z_OK,
		FLOOR_BACK_MIN,
		FLOOR_BACK_HOUR,
		FLOOR_BACK_DAY,
		NOT_CUSTOMER,
	}

	private static long TIME_STAMP_MIN = 0;
	private static long TIME_STAMP_HOUR = 0;
	
	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {

		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		try {
			TIME_STAMP_MIN = timeFormat.parse(
					context.getConfiguration().get(Common.FLOW_TIME_MIN)).getTime() - 1;
			TIME_STAMP_HOUR = timeFormat.parse(
					context.getConfiguration().get(Common.FLOW_TIME_HOUR)).getTime() - 1;
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		context.getCounter(JobCounter.MAP_OK).increment(1);
		if (cb.getUserType() != UserType.CUSTOMER) {
			context.getCounter(JobCounter.NOT_CUSTOMER).increment(1);
			return;
		}
		
		//TODO get mall id from floorId
		int mallId = Common.DEFAULT_MALL_ID;
		Flow.Builder fb = Flow.newBuilder();
		fb.setMallId(mallId);
		fb.addPhoneMac(cb.getPhoneMac());
		BytesWritable outVal = new BytesWritable(fb.build().toByteArray());

		
		long latestTime = 0;
		for (Location loc : cb.getLocationList()) {
			int index = loc.getTimeStampCount();
			long timeEnd = loc.getTimeStamp(index - 1);
			
			if (latestTime < timeEnd) {
				latestTime = timeEnd;
			}
		}
		
		if (latestTime > TIME_STAMP_MIN) {
			Text outKey = new Text(Common.FLOW_TAG_MIN +
					Common.CTRL_A + String.valueOf(mallId));
			context.write(outKey, outVal);
		}	
		if (latestTime > TIME_STAMP_HOUR) {
			Text outKey = new Text(Common.FLOW_TAG_HOUR +
					Common.CTRL_A + String.valueOf(mallId));
			context.write(outKey, outVal);
		}
		Text outKey = new Text(Common.FLOW_TAG_DAY +
				Common.CTRL_A + String.valueOf(mallId));
		context.write(outKey, outVal);
	}
}
