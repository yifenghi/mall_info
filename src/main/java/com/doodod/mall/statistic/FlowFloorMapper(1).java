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

public class FlowFloorMapper extends
	Mapper<Text, BytesWritable, Text, BytesWritable> {
	enum JobCounter {
		MAP_OK,
		MAP_Z_OK,
		FLOOR_BACK_MIN,
		FLOOR_BACK_HOUR,
		FLOOR_BACK_DAY,
		NOT_CUSTOMER,
		MALL_ID_ERROR,
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
		if (cb.getUserType() != UserType.CUSTOMER) {
			context.getCounter(JobCounter.NOT_CUSTOMER).increment(1);
			return;
		}
		
		long mallId = Common.getMallId(cb.build());
		if (mallId == 0) {
			context.getCounter(JobCounter.MALL_ID_ERROR).increment(1);
			return;
		}
		
		context.getCounter(JobCounter.MAP_OK).increment(1);
		
		Set<Long> floorSet = new HashSet<Long>();
		for (Location loc : cb.getLocationList()) {
			floorSet.add(loc.getPlanarGraph());
		}
		int floorVisits = floorSet.size();
			
		long planarGraph = -1;
		int locationIndex = cb.getLocationCount();
		
		Set<Long> floorMinSet = new HashSet<Long>();
		Set<Long> floorHourSet = new HashSet<Long>();
		Set<Long> floorDaySet = new HashSet<Long>();

		while ( locationIndex-- > 0) {
			Location loc = cb.getLocation(locationIndex);
			
			if (planarGraph != loc.getPlanarGraph()) {
				planarGraph = loc.getPlanarGraph();

				int index = loc.getTimeStampCount();
				long timeEnd = loc.getTimeStamp(index - 1);
				
				if (timeEnd >= TIME_STAMP_MIN) {
					if (floorMinSet.contains(planarGraph)) {
						context.getCounter(JobCounter.FLOOR_BACK_MIN).increment(1);
						continue;
					}
					
					Flow.Builder fb = Flow.newBuilder();
					floorMinSet.add(planarGraph);
					fb.setPlanarGraph(planarGraph);
					fb.addPhoneMac(cb.getPhoneMac());
					
					Text outKey = new Text(Common.FLOW_TAG_MIN +
							Common.CTRL_A + String.valueOf(planarGraph));
					BytesWritable outVal = new BytesWritable(fb.build().toByteArray());
					context.write(outKey, outVal);
				}
				
				if (timeEnd >= TIME_STAMP_HOUR) {
					if (floorHourSet.contains(planarGraph)) {
						context.getCounter(JobCounter.FLOOR_BACK_HOUR).increment(1);
						continue;
					}
					
					Flow.Builder fb = Flow.newBuilder();
					floorHourSet.add(planarGraph);
					fb.setPlanarGraph(planarGraph);
					fb.addPhoneMac(cb.getPhoneMac());
					
					Text outKey = new Text(Common.FLOW_TAG_HOUR +
							Common.CTRL_A + String.valueOf(planarGraph));
					BytesWritable outVal = new BytesWritable(fb.build().toByteArray());
					context.write(outKey, outVal);
				}
				
				if (floorDaySet.contains(planarGraph)) {
					context.getCounter(JobCounter.FLOOR_BACK_DAY).increment(1);
					continue;
				}
				
				Flow.Builder fb = Flow.newBuilder();
				floorHourSet.add(planarGraph);
				fb.setPlanarGraph(planarGraph);
				fb.addPhoneMac(cb.getPhoneMac());
				fb.addFloorVisits(floorVisits);
				
				Text outKey = new Text(Common.FLOW_TAG_DAY +
						Common.CTRL_A + String.valueOf(planarGraph));
				BytesWritable outVal = new BytesWritable(fb.build().toByteArray());
				context.write(outKey, outVal);		
			}	
		}
	}
}
