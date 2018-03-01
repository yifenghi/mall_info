package com.doodod.mall.statistic;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;
import com.doodod.mall.message.Mall.UserType;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class DwellShopMapper extends 
	Mapper<Text, BytesWritable, Text, Text> {
	enum JobCounter {
		CUST_MAC_CONFLICT,
		NOT_CUSTOMER,
	}
//	private Map<String, Map<Long, Long>> storeMap = new HashMap<String, Map<Long, Long>>(); 

	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		String custMac = new String(cb.getPhoneMac().toByteArray());
		if (cb.getUserType() != UserType.CUSTOMER) {
			context.getCounter(JobCounter.NOT_CUSTOMER).increment(1);
			return;
		}
		
//		Map<Long, Long> dwellMap = new HashMap<Long, Long>();
		for (Location loc : cb.getLocationList()) {
			int index = loc.getTimeStampCount();
			long timeDwell = loc.getTimeStamp(index - 1) 
					- loc.getTimeStamp(0) + 1;
			
			timeDwell /= Common.MINUTE_FORMATER;
			if (timeDwell == 0) {
				timeDwell ++;
			}
			
//			dwellMap.put((long)loc.getShopId(), timeDwell);
			Text outKey = new Text(Common.DWELL_TYPE_SHOP + 
					Common.CTRL_A + String.valueOf(loc.getShopId()));
			Text outVal = new Text(custMac + Common.CTRL_A
					+ String.valueOf(timeDwell));
			context.write(outKey, outVal);
		}
		
//		if (storeMap.containsKey(custMac)) {
//			context.getCounter(JobCounter.CUST_MAC_CONFLICT).increment(1);
//		}
//		storeMap.put(custMac, dwellMap);
	}
	
//	@Override
//	public void cleanup(
//			Mapper<Text, BytesWritable, Text, LongWritable>.Context context)
//			throws IOException, InterruptedException {
//		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
//		long createTime = 0;
//		try {
//			createTime = timeFormat.parse(
//					context.getConfiguration().get(Common.MALL_SYSTEM_BIZDATE)).getTime();
//		} catch (ParseException e) {
//			e.printStackTrace();
//		}
//		
//		String mongoServerList = context.getConfiguration().get(
//				Common.MONGO_SERVER_LIST);
//		String serverArr[] = mongoServerList.split(Common.COMMA, -1);
//		if (serverArr.length != Common.MONGO_SERVER_NUM) {
//			throw new RuntimeException("Get mongo server fail.");
//		}
//		String mongoServerFst = serverArr[0];
//		String mongoServerSnd = serverArr[1];
//		String mongoServerTrd = serverArr[2];
//
//		String mongoDbName = context.getConfiguration().get(
//				Common.MONGO_DB_NAME);
//		int mongoServerPort = Integer.parseInt(context.getConfiguration().get(
//				Common.MONGO_SERVER_PORT));
//
//		MongoClient mongoClient = new MongoClient(Arrays.asList(
//				new ServerAddress(mongoServerFst, mongoServerPort),
//				new ServerAddress(mongoServerSnd, mongoServerPort),
//				new ServerAddress(mongoServerTrd, mongoServerPort)));
//		DB mongoDb = mongoClient.getDB(mongoDbName);
//		
//		String dwellCollectionName = context.getConfiguration().get(
//				Common.MONGO_COLLECTION_DWELL);
//		DBCollection dwellCollection = 
//				mongoDb.getCollection(dwellCollectionName);
//		
//		String dwellKeyUser = context.getConfiguration().get(
//				Common.MONGO_COLLECTION_DWELL_USER);
//		String dwellKeyPos = context.getConfiguration().get(
//				Common.MONGO_COLLECTION_DWELL_POS);
//		String dwellKeyType = context.getConfiguration().get(
//				Common.MONGO_COLLECTION_DWELL_TYPE);
//		String dwellKeyTime = context.getConfiguration().get(
//				Common.MONGO_COLLECTION_DWELL_TIME);
//		String dwellNum = context.getConfiguration().get(
//				Common.MONGO_COLLECTION_DWELL_NUMBER);
//		
//		Iterator<String> userIdIter = storeMap.keySet().iterator();
//		while (userIdIter.hasNext()) {
//			String userId = userIdIter.next();
//			
//			Map<Long, Long> dwellMap = storeMap.get(userId);
//			Iterator<Long> storeIter = dwellMap.keySet().iterator();
//
//			while (storeIter.hasNext()) {
//				long storeId = storeIter.next();
//				long timeDwell = dwellMap.get(storeId);
//				
//	            BasicDBObject document = new BasicDBObject();
//	            document.put(dwellKeyUser, userId);
//	            document.put(dwellKeyPos, storeId);
//	            document.put(dwellKeyType, Common.DWELL_TYPE_SHOP);
//	            document.put(dwellKeyTime, createTime);
//	            document.put(dwellNum, timeDwell);
//	            
//	            BasicDBObject query = new BasicDBObject();
//	            query.put(dwellKeyUser, userId);
//	            query.put(dwellKeyPos, storeId);
//	            query.put(dwellKeyType, Common.DWELL_TYPE_SHOP);
//	            query.put(dwellKeyTime, createTime);
//	            
//	            dwellCollection.update(query, document, true, false);				
//			}
//		}
//		
//		mongoClient.close();
//	}
}
