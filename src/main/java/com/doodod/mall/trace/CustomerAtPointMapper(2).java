package com.doodod.mall.trace;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;
import com.doodod.mall.message.Mall.UserType;

public class CustomerAtPointMapper 
	extends Mapper<Text, BytesWritable, Text, LongWritable> {
	enum JobCounter {
		NOT_CUSTOMER,
		MALL_ID_ERROR,
	}
	
	private static long TIME_STAMP_MIN = 0;

	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		try {
			TIME_STAMP_MIN = timeFormat.parse(
					context.getConfiguration().get(Common.FLOW_TIME_MIN)).getTime() - 1;
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());

		if (cb.getUserType() != UserType.CUSTOMER) {
			context.getCounter(JobCounter.NOT_CUSTOMER).increment(1);
			return;
		}
		
		int mallId = Common.getMallId(cb.build());
		if (mallId == 0) {
			context.getCounter(JobCounter.MALL_ID_ERROR).increment(1);
			return;
		}
		
		long timeMax = 0;
		int index = 0;
		for (int i = 0; i < cb.getLocationCount(); i++) {
			Location loc = cb.getLocation(i);
			int timeListSize = loc.getTimeStampCount();
			long latestTime = loc.getTimeStamp(timeListSize - 1);
			if (timeMax < latestTime) {
				timeMax = latestTime;
				index = i;
			}
		}
		
		if (timeMax < TIME_STAMP_MIN) {
			return;
		}
		
		Location latestLoc = cb.getLocation(index);
		
		long floorId = latestLoc.getPlanarGraph();
		double x = latestLoc.getLocationX();
		double y = latestLoc.getLocationY();
		
		Text outKey = new Text(floorId + Common.CTRL_A +
				x + Common.CTRL_A + y);
		context.write(outKey, new LongWritable(1));
		
	}

}
