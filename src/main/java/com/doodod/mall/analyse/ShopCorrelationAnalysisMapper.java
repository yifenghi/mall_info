package com.doodod.mall.analyse;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.common.Coordinates;
import com.doodod.mall.common.Point;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;
import com.doodod.mall.message.Mall.UserType;
import com.doodod.mall.message.Mall.Visit;
public class ShopCorrelationAnalysisMapper extends
		Mapper<Text, BytesWritable, Text, Text> {
	enum JobCounter {
		NOT_CUSTOMER,
		STORE_FROMAT_ERROR,
		NO_VISIT_SHOP,
		TEST1,
		CUSTOMER,
		READ_SHOP_LIST_OK,
		TEST2
	}
	List<Integer> StastisticShopList=new ArrayList<Integer>();
	@Override
	protected void map(Text key, BytesWritable value,Context context)
			throws IOException, InterruptedException {

		Customer.Builder cb = Customer.newBuilder();
		cb.mergeFrom(value.getBytes(),0,value.getLength());
		if (cb.getUserType() != UserType.CUSTOMER) {
			context.getCounter(JobCounter.NOT_CUSTOMER).increment(1);	
			return;
		}else{
			context.getCounter(JobCounter.CUSTOMER).increment(1);
		}
		if(cb.getLocationList().isEmpty()){
			context.getCounter(JobCounter.NO_VISIT_SHOP).increment(1);	
			return;	
		}
		
		
		List<Long> dateList = new ArrayList<Long>();
		for(Location.Builder lb : cb.getLocationBuilderList()){
			dateList.add(lb.getTimeStamp(0));
		}
		Collections.sort(dateList);
		List<Integer> shopIdList = new ArrayList<Integer>(); 
		for(long date : dateList){
			for(Location.Builder lb : cb.getLocationBuilderList()){
				
				if(date == lb.getTimeStamp(0)){				
					shopIdList.add(lb.getShopId());
				}
			}
		}
		context.getCounter(JobCounter.TEST1).increment(1);	
		
		for(Integer NeedStatisticShop : StastisticShopList){
			
			for(int i=0;i<shopIdList.size();i++){
				if(shopIdList.get(i).intValue()==NeedStatisticShop.intValue()){
					context.getCounter(JobCounter.TEST2).increment(1);
					
					for(int j=i;j<shopIdList.size();j++){
						if(shopIdList.get(j).intValue() != NeedStatisticShop.intValue()){
							context.write(new Text(""+NeedStatisticShop), new Text(""+shopIdList.get(j)));
						}
						
					}					
				}
			}
		}
	
		
		
		
	}
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		
		Charset charSet = Charset.forName("UTF-8");
		
		String storePath = context.getConfiguration().get(Common.SHOP_CONF_STORE);
		BufferedReader storeReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(storePath), charSet));
		String line = "";
		while ((line = storeReader.readLine()) != null) {
			String[] arrBrand = line.split(Common.CTRL_A, -1);
			if (arrBrand.length != 5) {
				context.getCounter(JobCounter.STORE_FROMAT_ERROR).increment(1);		
				continue;
			}
			StastisticShopList.add(Integer.parseInt(arrBrand[1]));
			context.getCounter(JobCounter.READ_SHOP_LIST_OK).increment(1);	
			
		}
		storeReader.close();
	}

}
