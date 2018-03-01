package com.doodod.mall.visit;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.UserType;

public class VisitPhoneBrandMapper extends 
	Mapper<Text, BytesWritable, Text, LongWritable> {
	enum JobCounter {
		NO_PHONE_BRAND,
		BRAND_UNKOWN,
		BRAND_OTHER,
		NOT_CUSTOMER,
		MALL_ID_ERROR,
	}
	private static Set<String> PHONE_BRAND_SET = new HashSet<String>();
	
	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		Charset charSet = Charset.forName("UTF-8");
		String brandListPath = context.getConfiguration().get(
				Common.CONF_BRAND_LIST);
		BufferedReader brandReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(brandListPath), charSet));
		String line = "";
		while ((line = brandReader.readLine()) != null) {
			String arr[] = line.split(Common.CTRL_A, -1);
			if (arr.length < 2) {
				continue;
			}
			PHONE_BRAND_SET.add(arr[1]);
		}
		brandReader.close();
	}

	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		if (!cb.hasPhoneBrand()) {
			context.getCounter(JobCounter.NO_PHONE_BRAND).increment(1);
			return;
		}
		
		if (cb.getUserType() != UserType.CUSTOMER) {
			context.getCounter(JobCounter.NOT_CUSTOMER).increment(1);
			return;
		}
		
		int mallId = Common.getMallId(cb.build());
		if (mallId == 0) {
			context.getCounter(JobCounter.MALL_ID_ERROR).increment(1);
			return;
		}
		
		String phoneBrand = new String(cb.getPhoneBrand().toByteArray());
		if (phoneBrand.equals(Common.BRAND_UNKNOWN)) {
			context.getCounter(JobCounter.BRAND_UNKOWN).increment(1);
			phoneBrand = Common.DEFAULT_MAC_BRAND;
		}
		else if (!PHONE_BRAND_SET.contains(phoneBrand)) {
			context.getCounter(JobCounter.BRAND_OTHER).increment(1);
			phoneBrand = Common.DEFAULT_MAC_BRAND;
		}
		
		context.write(new Text(phoneBrand + 
				Common.CTRL_A + mallId), new LongWritable(1));	
	}

}
