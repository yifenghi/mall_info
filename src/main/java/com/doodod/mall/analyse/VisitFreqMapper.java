package com.doodod.mall.analyse;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Visit;

public class VisitFreqMapper extends 
	Mapper<Text, BytesWritable, Text, Text> {
	enum JobCounter {
		PERIOD_LIMIT_TOATL_COUNT,
		PERIOD_LIMIT_RECENT_COUNT,
	}
	
	public static long DATE_MONTH_AGO = 0;
	public static long PERIOD_LIMIT_TOTAL = 
			Common.DEFAULT_PERIOD_TOTAL * Common.DAY_FORMATER;
	public static long PERIOD_LIMIT_RECENT = 
			Common.DEFAULT_PERIOD_RECENT * Common.DAY_FORMATER;
	public static double PERIOD_LIMIT_FILTER = Common.DEFAULT_PERIOD_FLITER; 
	public static String EMPLOYEE_TAG = "EMPLOYEE";
	public static String NORMAL_TAG = "NORMAL";

	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		try {
			DATE_MONTH_AGO = timeFormat.parse(
					context.getConfiguration().get(Common.MALL_SYSTEM_BIZDATE)).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		PERIOD_LIMIT_TOTAL = context.getConfiguration().getInt(
				Common.VISIT_PERIOD_TOTAL, Common.DEFAULT_PERIOD_TOTAL)
				* Common.DAY_FORMATER;
		PERIOD_LIMIT_RECENT = context.getConfiguration().getInt(
				Common.VISIT_PERIOD_RECENT, Common.DEFAULT_PERIOD_RECENT) 
				* Common.DAY_FORMATER;
		PERIOD_LIMIT_FILTER = context.getConfiguration().getDouble(
				Common.VISIT_PERIOD_FILTER, Common.DEFAULT_PERIOD_FLITER);
	}
	
	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		
		for (Visit visit : cb.getUserVisitList()) {
			int mallId = visit.getMallId();
			int count = visit.getVisitDateCount();
			int index = 0;
			List<Long> visitList = visit.getVisitDateList();
			
			long period = visitList.get(count -1) - visitList.get(0);
			if (period < PERIOD_LIMIT_TOTAL) {
				context.getCounter(JobCounter.PERIOD_LIMIT_TOATL_COUNT).increment(1);
				continue;
			}
			
			for (int i = 0; i < visitList.size(); i++) {
				if (DATE_MONTH_AGO < visitList.get(i)) {
					index = i;
					break;
				}
			}
			
			double periodAtMonth = (visitList.get(count -1) - visitList.get(index));
			if (periodAtMonth < PERIOD_LIMIT_RECENT) {
				context.getCounter(JobCounter.PERIOD_LIMIT_RECENT_COUNT).increment(1);
				continue;
			}
					
			double freqAtMonth = (count - index) / 
					(periodAtMonth / (1.0 * Common.DAY_FORMATER) + 1);
			
			String tag = EMPLOYEE_TAG;
			if (freqAtMonth < PERIOD_LIMIT_FILTER) {
				tag = NORMAL_TAG;
			}
			Text outKey = new Text(mallId + Common.CTRL_A 
					+ key.toString());
			DecimalFormat df = new DecimalFormat(Common.NUM_FORNAT);
			Text outVal = new Text(tag +
					Common.CTRL_A + df.format(freqAtMonth) + 
					Common.CTRL_A + index + 
					Common.CTRL_A + count + 
					Common.CTRL_A + periodAtMonth / Common.DAY_FORMATER);
			
			context.write(outKey, outVal);
			
		}
	}

}
