package com.doodod.mall.statistic;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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

public class DwellFloorMapper extends 
	Mapper<Text, BytesWritable, Text, Text> {
	enum JobCounter {
		CUST_MAC_CONFLICT,
		NOT_CUSTOMER,
	}
	//private Map<String, Map<Long, Long>> floorMap = new HashMap<String, Map<Long, Long>>();
	
	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		
		if (cb.getUserType() != UserType.CUSTOMER) {
			context.getCounter(JobCounter.NOT_CUSTOMER).increment(1);
			return;
		}
		
		Map<Long, Location.Builder> floorBuilderMap = new HashMap<Long, Location.Builder>(); 
		String custMac = new String(cb.getPhoneMac().toByteArray());
		for (Location loc : cb.getLocationList()) {
			long floorId = loc.getPlanarGraph();
			if (floorBuilderMap.containsKey(floorId)) {
				Location.Builder lcb = floorBuilderMap.get(floorId);
				for (long time : loc.getTimeStampList()) {
					lcb.addTimeStamp(time);
				}
			}
			else {
				Location.Builder lcb = Location.newBuilder();
				lcb.mergeFrom(loc).clearLocationX().clearLocationY();
				floorBuilderMap.put(floorId, lcb);
			}
		}
		
		//Map<Long, Long> dwellMap = new HashMap<Long, Long>();
		Iterator<Long> floorIter = floorBuilderMap.keySet().iterator();
		while (floorIter.hasNext()) {
			long floorId = floorIter.next();
			Location.Builder lcb = floorBuilderMap.get(floorId);
			List<Long> timeList = new ArrayList<Long>(lcb.getTimeStampList());
			Collections.sort(timeList);
			
			long timeDwell = timeList.get(timeList.size() -1)
					- timeList.get(0) + 1;
			timeDwell /= Common.MINUTE_FORMATER;
			
			if (timeDwell == 0) {
				timeDwell ++;
			}
			
			//dwellMap.put(floorId, timeDwell);
			Text outKey = new Text(Common.DWELL_TYPE_FLOOR
					+ Common.CTRL_A + String.valueOf(floorId));
			Text outVal = new Text(custMac
					+ Common.CTRL_A + String.valueOf(timeDwell));
			context.write(outKey, outVal);
		}
		
		//map block contains several customer builders, deal a part each time;
		//there will be no conflict.
//		if (floorMap.containsKey(custMac)) {
//			context.getCounter(JobCounter.CUST_MAC_CONFLICT).increment(1);
//		}
//		floorMap.put(custMac, dwellMap);
	}
	
//	@Override
//	public void cleanup(Context context)
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
//		Iterator<String> userIdIter = floorMap.keySet().iterator();
//		while (userIdIter.hasNext()) {
//			String userId = userIdIter.next();
//			
//			Map<Long, Long> dwellMap = floorMap.get(userId);
//			
//			Iterator<Long> floorIter = dwellMap.keySet().iterator();
//			while (floorIter.hasNext()) {
//				long floorId = floorIter.next();
//				long timeDwell = dwellMap.get(floorId);
//				
//	            BasicDBObject document = new BasicDBObject();
//	            document.put(dwellKeyUser, userId);
//	            document.put(dwellKeyPos, floorId);
//	            document.put(dwellKeyType, Common.DWELL_TYPE_FLOOR);
//	            document.put(dwellKeyTime, createTime);
//	            document.put(dwellNum, timeDwell);
//	            
//	            BasicDBObject query = new BasicDBObject();
//	            query.put(dwellKeyUser, userId);
//	            query.put(dwellKeyPos, floorId);
//	            query.put(dwellKeyType, Common.DWELL_TYPE_FLOOR);
//	            query.put(dwellKeyTime, createTime);
//	            
//	            dwellCollection.update(query, document, true, false);				
//			}
//
//		}
//		
//		mongoClient.close();
//	}

}
