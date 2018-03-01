package com.doodod.mall.trace;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;
import com.doodod.mall.message.Mall.UserType;

public class CustmerAtPointMapper 
	extends Mapper<Text, BytesWritable, Text, LongWritable> {
	enum JobCounter {
		NOT_CUSTOMER,
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
		
		Location latestLoc = cb.getLocation(index);
		
		long floorId = latestLoc.getPlanarGraph();
		double x = latestLoc.getLocationX();
		double y = latestLoc.getLocationY();
		
		Text outKey = new Text(floorId + Common.CTRL_A +
				x + Common.CTRL_A + y);
		context.write(outKey, new LongWritable(1));
		
	}

}
