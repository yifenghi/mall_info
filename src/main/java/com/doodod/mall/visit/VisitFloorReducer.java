package com.doodod.mall.visit;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Visit;

public class VisitFloorReducer extends
		Reducer<Text, BytesWritable, Text, BytesWritable> {
	enum JobCounter {
		TOTAL_REDUCE_OK, 
		PART_REDUCE_OK, 
		INVALID_RECORD, 
		NEW_CUSTOMER, 
		OLD_CUSTOMER,
	}

	@Override
	public void reduce(Text key, Iterable<BytesWritable> values, Context context)
			throws IOException, InterruptedException {
		Customer.Builder total = Customer.newBuilder();
		Customer.Builder part = Customer.newBuilder();

		for (BytesWritable val : values) {
			byte[] arr = val.getBytes();
			if (arr[0] == Common.MERGE_TAG_T) {
				total.clear().mergeFrom(val.getBytes(), 1, val.getLength() - 1);
				context.getCounter(JobCounter.TOTAL_REDUCE_OK).increment(1);
			} else if (arr[0] == Common.MERGE_TAG_P) {
				part.clear().mergeFrom(val.getBytes(), 1, val.getLength() - 1);
				context.getCounter(JobCounter.PART_REDUCE_OK).increment(1);
			} else {
				context.getCounter(JobCounter.INVALID_RECORD).increment(1);
			}
		}

		if (!total.hasPhoneMac()) {
			context.getCounter(JobCounter.NEW_CUSTOMER).increment(1);
			context.write(key, new BytesWritable(part.build().toByteArray()));
			return;
		}

		if (!part.hasPhoneMac()) {
			context.getCounter(JobCounter.OLD_CUSTOMER).increment(1);
			context.write(key, new BytesWritable(total.build().toByteArray()));
			return;
		}

		HashMap<Long, Visit> floorMap = new HashMap<Long, Visit>();
		for (Visit visit : part.getUserVisitList()) {
			floorMap.put(visit.getPlanarGraph(), visit);
		}

		Set<Long> floorVisited = new HashSet<Long>();
		for (Visit.Builder visit : total.getUserVisitBuilderList()) {
			long floorId = visit.getPlanarGraph();
			if (floorMap.containsKey(floorId)) {
				Visit newVisit = floorMap.get(floorId);
				visit.mergeFrom(newVisit);
				floorVisited.add(floorId);
			}
		}

		Iterator<Long> iter = floorMap.keySet().iterator();
		while (iter.hasNext()) {
			long floorId = iter.next();
			if (floorVisited.contains(floorId)) {
				continue;
			}
			total.addUserVisit(floorMap.get(floorId));
		}

		context.write(key, new BytesWritable(total.build().toByteArray()));
	}
}
