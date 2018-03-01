package com.doodod.mall.statistic;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.doodod.mall.common.Common;
import com.doodod.mall.common.Coordinates;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;
import com.doodod.mall.message.Mall.UserType;
import com.doodod.mall.statistic.ShopMapper.JobCounter;

public class FlowMallShopMapper extends 
	Mapper<Text, BytesWritable, Text, Text> {
	enum JobCounter {
		NOT_CUSTOMER,
		MALL_ID_ERROR,
		PUBLIC_AREA_ERROR
	}
	private static Set<Integer> publicArea = new HashSet<Integer>();//public area category_id
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Charset charSet = Charset.forName("UTF-8");
		
		String publicPath = context.getConfiguration().get(Common.SHOP_CONF_PUBLIC_AREA);
		BufferedReader publicReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(publicPath), charSet));
		String line = "";
		while ((line = publicReader.readLine()) != null) {
			String[] arrFrame = line.split(Common.CTRL_A, -1);
			if (arrFrame.length != 3) {
				context.getCounter(JobCounter.PUBLIC_AREA_ERROR).increment(1);		
				continue;
			}
			
			publicArea.add(Integer.parseInt(arrFrame[0]));
		}
		publicReader.close();

		
		
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
		
		int mallId = (int) Common.getMallId(cb.build());
		if (mallId == 0) {
			context.getCounter(JobCounter.MALL_ID_ERROR).increment(1);
			return;
		}
		int shopNum = 0;
		Set<Integer> zoneSet = new HashSet<Integer>();
		for(Location loc : cb.getLocationList()) {
			zoneSet.add(loc.getShopCat());
			//add by lifeng :drive out the public area
			if(publicArea.contains(loc.getShopCat())){
				continue;
			}else{
				shopNum++;
			}
		}
		
		int zoneNum = zoneSet.size();
		Text outKey = new Text(String.valueOf(mallId));
		Text outVal = new Text(shopNum + Common.CTRL_A + zoneNum);
		context.write(outKey, outVal);
	}
}
