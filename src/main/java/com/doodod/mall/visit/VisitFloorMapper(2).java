package com.doodod.mall.visit;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;
import com.doodod.mall.message.Mall.UserType;
import com.doodod.mall.message.Mall.Visit;

public class VisitFloorMapper extends
		Mapper<Text, BytesWritable, Text, BytesWritable> {
	enum JobCounter {
		NOT_CUSTOMER,
		MALL_ID_ERROR,
	}
	
	private static long DATE_TIME = 0;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		try {
			DATE_TIME = timeFormat.parse(
					context.getConfiguration().get(Common.MALL_SYSTEM_TODAY))
					.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void map(Text key, BytesWritable val, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(val.getBytes(), 0, val.getLength());
		
		if (cb.getUserType() != UserType.CUSTOMER) {
			context.getCounter(JobCounter.NOT_CUSTOMER).increment(1);
			return;
		}
		
		int mallId = Common.getMallId(cb.build());
		if (mallId == 0) {
			context.getCounter(JobCounter.MALL_ID_ERROR).increment(1);
			return;
		}
		
		Set<Long> floorSet = new HashSet<Long>();
		for (Location loc : cb.getLocationList()) {
			floorSet.add(loc.getPlanarGraph());
		}

		Iterator<Long> iter = floorSet.iterator();
		while (iter.hasNext()) {
			long floorId = iter.next();
			Visit.Builder vb = Visit.newBuilder();
			vb.setPlanarGraph(floorId);
			vb.addVisitDate(DATE_TIME);
			
			cb.addUserVisit(vb.build());
		}

		cb.clearLocation();

		byte[] arr = cb.build().toByteArray();
		byte[] res = new byte[arr.length + 1];
		int index = 0;
		res[index++] = Common.MERGE_TAG_P;
		while (index < res.length) {
			res[index] = arr[index - 1];
			index++;
		}

		BytesWritable out = new BytesWritable(res);
		context.write(key, out);
	}
}
