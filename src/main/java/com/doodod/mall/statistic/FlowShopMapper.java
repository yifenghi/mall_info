/**
 * @author yupeng.cao@palmaplus.com
 */
package com.doodod.mall.statistic;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Flow;
import com.doodod.mall.message.Mall.Location;
import com.doodod.mall.message.Mall.UserType;

public class FlowShopMapper extends 
	Mapper<Text, BytesWritable, Text, BytesWritable> {
	enum JobCounter {
		NOT_CUSTOMER,
		MAP_OK,
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
		
		int shopVisits = cb.getLocationCount();
		Set<Integer> zoneSet = new HashSet<Integer>();
		for (Location loc : cb.getLocationList()) {
			zoneSet.add(loc.getShopCat());
		}
		int zoneVisits = zoneSet.size();
		
		int locationIndex = cb.getLocationCount();
		while ( locationIndex-- > 0) {
			Location loc = cb.getLocation(locationIndex);
			
			int index = loc.getTimeStampCount();
			long timeEnd = loc.getTimeStamp(index - 1);
			
			if (timeEnd >= TIME_STAMP_MIN) {
				Flow.Builder fb = Flow.newBuilder();
				fb.setShopId(loc.getShopId());
				fb.setShopCat(loc.getShopCat());
				fb.addPhoneMac(cb.getPhoneMac());

				Text outKey = new Text(Common.FLOW_TAG_MIN + 
						Common.CTRL_A + String.valueOf(loc.getShopId()));
				context.write(outKey, new BytesWritable(fb.build().toByteArray()));
			}
			
			if (timeEnd >= TIME_STAMP_HOUR) {
				Flow.Builder fb = Flow.newBuilder();
				fb.setShopId(loc.getShopId());
				fb.setShopCat(loc.getShopCat());
				fb.addPhoneMac(cb.getPhoneMac());

				Text outKey = new Text(Common.FLOW_TAG_HOUR + 
						Common.CTRL_A + String.valueOf(loc.getShopId()));
				context.write(outKey, new BytesWritable(fb.build().toByteArray()));
			}
			
			Flow.Builder fb = Flow.newBuilder();
			fb.setShopId(loc.getShopId());
			fb.setShopCat(loc.getShopCat());
			fb.addPhoneMac(cb.getPhoneMac());
			fb.addShopVisits(shopVisits);
			fb.addZoneVisits(zoneVisits);

			Text outKey = new Text(Common.FLOW_TAG_DAY + 
					Common.CTRL_A + String.valueOf(loc.getShopId()));
			context.write(outKey, new BytesWritable(fb.build().toByteArray()));
					
		}
	}

}
