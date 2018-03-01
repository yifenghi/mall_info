package com.doodod.mall.visit;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.UserType;
import com.doodod.mall.message.Mall.Visit;

public class VisitMallMapper extends 
	Mapper<Text, BytesWritable, Text, BytesWritable> {
	enum JobCounter {
		NOT_CUSTOMER,
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
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		//TODO get mall_id from floorid
		if (cb.getUserType() != UserType.CUSTOMER) {
			context.getCounter(JobCounter.NOT_CUSTOMER).increment(1);
			return;
		}
		
		Visit.Builder vb = Visit.newBuilder();
		vb.setMallId(Common.DEFAULT_MALL_ID);
		vb.addVisitDate(DATE_TIME);
		cb.clearLocation().addUserVisit(vb.build());
		
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
