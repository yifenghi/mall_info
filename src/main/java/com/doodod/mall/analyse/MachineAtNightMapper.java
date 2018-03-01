package com.doodod.mall.analyse;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.common.TimeCluster;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.UserType;

public class MachineAtNightMapper extends 
	Mapper<Text, BytesWritable, Text, Text> {
	
	private static long TIME_STAMP_NIGHT = 0;
	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		try {
			TIME_STAMP_NIGHT = timeFormat.parse(
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
			return;
		}
		
		int mallId = Common.getMallId(cb.build());
		if (mallId == 0) {
			return;
		}
		
		long timeEarliest = TimeCluster.getMinTimeStamp(cb.build());
		if (timeEarliest > TIME_STAMP_NIGHT ) {
			return;
		}
		
		long dwell = TimeCluster.getTimeDwell(
				cb.build(), Common.DEFAULT_DWELL_PASSENGER) / Common.MINUTE_FORMATER;
		
		Text keyOut = new Text(mallId + Common.CTRL_A + key.toString());
		Text valOut = new Text(String.valueOf(dwell));
		context.write(keyOut, valOut);
	}
}
