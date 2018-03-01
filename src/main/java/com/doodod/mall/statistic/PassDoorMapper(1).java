/**
 * @author yanshang.gong@palmaplus.com
 */
package com.doodod.mall.statistic;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Customer;

public class PassDoorMapper extends Mapper<Text, BytesWritable, Text, Text> {
	enum JobCounter {
		PARA_ERROR1, 
		PARA_ERROR2, 
		PARA_ERROR3, 
		PARA_ERROR_FILEPATH, 
		READFILE_ERROR,
		JSON_ERROR, 
		ALL_CUSTOMER_COUNT, 
		VALID_CUSTOMER_COUNT, 
		NULL_CUSTOMER_COUNT, 
		ZERO_DOORS,
		MALL_ID_ERROR,
	}

	private static long TIME_START;
	private static long TIME_END;
	private NearDoor nd;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			NearDoor.NEAR_LIMIT = context.getConfiguration().getDouble(
					Common.NEARDOOR_LIMIT, 2.0);

		} catch (Exception e) {
			context.getCounter(JobCounter.PARA_ERROR1).increment(1);
		}
		try {

			TIME_START = sdf.parse(
					context.getConfiguration().get(Common.NEARDOOR_TIME_START))
					.getTime();

		} catch (Exception e) {
			context.getCounter(JobCounter.PARA_ERROR2).increment(1);
		}
		try {

			TIME_END = sdf.parse(
					context.getConfiguration().get(Common.NEARDOOR_TIME_END))
					.getTime();
		} catch (Exception e) {
			context.getCounter(JobCounter.PARA_ERROR3).increment(1);
		}

		String filePath = context.getConfiguration().get(Common.FLOORINFO_PATH);
		try {
			nd = new NearDoor(filePath);
		} catch (Exception e) {
			context.getCounter(JobCounter.PARA_ERROR_FILEPATH).increment(1);
		}
		if (nd.getDoorInfo().getDoorNum() == 0) {
			context.getCounter(JobCounter.ZERO_DOORS).increment(1);
		}
	}

	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		Customer cus = cb.build();
		if (cus.getLocationCount() == 0) {
			context.getCounter(JobCounter.NULL_CUSTOMER_COUNT).increment(1);
			return;
		}
		context.getCounter(JobCounter.ALL_CUSTOMER_COUNT).increment(1);
		long mallId = Common.getMallId(cb.build());
		if (mallId == 0) {
			context.getCounter(JobCounter.MALL_ID_ERROR).increment(1);
			return;
		}
		
		// 输出在特定时间段内的所有人员的出入门情况
		NearDoor.CustomerSorted cs = nd.getCustomerSorted(cus);
		// NearDoor.PassDoor pd = nd.getPassDoor(cus);
		NearDoor.PassDoor pd = nd.getPassDoor(cus, TIME_START, TIME_END);
		if (pd.getPassCount() > 0) {
			context.getCounter(JobCounter.VALID_CUSTOMER_COUNT).increment(1);
			System.out.println("mac:" + cs.getMac());
			for (int i = 0; i < pd.getPassCount(); i++) {
				String outputValue = String.valueOf(pd.getTimeStamp(i))
						+ Common.CTRL_A + String.valueOf(pd.getInOrOut(i));
				context.write(new Text(String.valueOf(mallId)
						+ Common.CTRL_A + pd.getDoorName(i)), new Text(
						outputValue));
			}
		}
	}
}
