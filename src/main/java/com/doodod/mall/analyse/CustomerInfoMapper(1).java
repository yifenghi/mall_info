package com.doodod.mall.analyse;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;

public class CustomerInfoMapper extends 
	Mapper<Text, BytesWritable, Text, Text> {
	private static Set<String> customerSet = new HashSet<String>();
	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		Charset charSet = Charset.forName("UTF-8");

		String publicPath = context.getConfiguration().get(Common.ANALYSE_CUSTOMER_MAC);
		BufferedReader publicReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(publicPath), charSet));
		String line = "";
		while ((line = publicReader.readLine()) != null) {
			customerSet.add(line.trim());
		}
		publicReader.close();
	}
	
	@Override
	public void map(Text key, BytesWritable value,
			Mapper<Text, BytesWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		
		if (!customerSet.contains(key.toString())) {
			return;
		}
		
		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		
		StringBuffer customer = new StringBuffer();
		customer.append(cb.getUserType().toString()).append(Common.CTRL_A);
		for (Location loc : cb.getLocationList()) {
			int timeListSize = loc.getTimeStampCount();
			String timeStart = timeFormat.format(new Date(loc.getTimeStamp(0)));
			String timeEnd = timeFormat.format(new Date(loc.getTimeStamp(timeListSize - 1)));
			
			customer.append(loc.getPlanarGraph()).append(Common.CTRL_B)
				.append(timeStart).append(Common.CTRL_B)
				.append(timeEnd).append(Common.CTRL_A);
		}
		
		customer.deleteCharAt(customer.length() - 1);
		context.write(key, new Text(customer.toString()));
	}

}
