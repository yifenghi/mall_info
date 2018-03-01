package com.doodod.mall.analyse;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import com.doodod.mall.common.Common;



public class ShopCorrelationAnalysisReducer extends
		Reducer<Text, Text, Text, Text> {
	
	enum JobCounter {
		READ_SHOP_NAME_OK,
		STORE_FROMAT_ERROR
	}
	
	
	Map<String,String> StastisticShopNameMap = new HashMap<String, String>();
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {

		Map<String,Integer> countMap = new HashMap<String,Integer>();
		
		for(Text val : values){
			String shopid = val.toString();
			if(countMap.containsKey(shopid)){
				int count = countMap.get(shopid);
				count++;
				countMap.put(shopid, count);
			}else{
				countMap.put(shopid, 1);
			}
			
		}
	
		////
		
		List<String> keyList = new LinkedList<String>();
		keyList.addAll(countMap.keySet());
		List<Integer> valueList = new LinkedList<Integer>();
		valueList.addAll(countMap.values());
		for (int i = 0; i < valueList.size(); i++){
			for (int j = i + 1; j < valueList.size(); j++) {
				if (valueList.get(j) > valueList.get(i)) {
					Integer val = valueList.get(j);
					valueList.set(j, valueList.get(i));
					valueList.set(i, val);
					// 同样调整对应的key值
					String key_count = keyList.get(j);
					keyList.set(j, keyList.get(i));
					keyList.set(i, key_count);
				}
			}
		}
		
		StringBuffer sb = new StringBuffer();
		sb.delete(0, sb.length());
		
		for(int i=0;i<keyList.size();i++){
			String shopid = keyList.get(i);
			sb.append(shopid);
			//sb.append(Common.CTRL_A);
			sb.append(",");
			sb.append(valueList.get(i));
			//sb.append(Common.CTRL_A);
			sb.append(",");
			if(StastisticShopNameMap.containsKey(shopid)){
				sb.append(StastisticShopNameMap.get(shopid));
			//sb.append(Common.CTRL_B);			
				sb.append("###");	
			}else{
				sb.append("null");
			//sb.append(Common.CTRL_B);			
				sb.append("###");				
			}
		}
		
		///
		
		
		
	/*	StringBuffer sb = new StringBuffer();
		sb.delete(0, sb.length());
		Iterator<String> it = countMap.keySet().iterator();				
		while (it.hasNext()) {
			String shopid = it.next();
			sb.append(shopid);
			//sb.append(Common.CTRL_A);
			sb.append(",");
			sb.append(countMap.get(shopid));
			//sb.append(Common.CTRL_A);
			sb.append(",");
			if(StastisticShopNameMap.containsKey(shopid)){
				sb.append(StastisticShopNameMap.get(shopid));
			//sb.append(Common.CTRL_B);			
				sb.append("###");	
			}else{
				sb.append("null");
			//sb.append(Common.CTRL_B);			
				sb.append("###");				
			}
			
		}	
		
		*/
		
		
		StringBuffer shopIdAndName = new StringBuffer(key.toString());
		shopIdAndName.append("&&");
		shopIdAndName.append(StastisticShopNameMap.get(key.toString()));
		
		
		context.write(new Text(shopIdAndName.toString()), new Text(sb.toString()));
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
			StastisticShopNameMap.put(arrBrand[1], arrBrand[2]);
			context.getCounter(JobCounter.READ_SHOP_NAME_OK).increment(1);	
			
		}
		storeReader.close();
	}
		
		
}
