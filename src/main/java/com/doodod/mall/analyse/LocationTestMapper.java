package com.doodod.mall.analyse;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;

public class LocationTestMapper extends Mapper<Text, BytesWritable, LongWritable, Text> {

	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		
		for (Location loc : cb.getLocationList()) {
			long floorId = loc.getPlanarGraph();
			double x = loc.getLocationX();
			double y = loc.getLocationY();
			
			String outX = "";
			String outY = "";
			if (x < 5000000) {
				outX = 1 + Common.CTRL_B + 0;
			}
			else {
				outX = 0 + Common.CTRL_B + 1;
			}
			
			if (y < 3000000) {
				outY = 1 + Common.CTRL_B + 0;
			}
			else {
				outY = 0 + Common.CTRL_B + 1;
			}
			
			LongWritable outKey = new LongWritable(floorId);
			Text outVal = new Text(outX + Common.CTRL_A + outY);
			context.write(outKey, outVal);
		}

	}
}
