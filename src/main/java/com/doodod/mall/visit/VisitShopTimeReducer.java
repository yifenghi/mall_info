package com.doodod.mall.visit;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.doodod.mall.common.Common;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class VisitShopTimeReducer extends 
	Reducer<Text, Text, Text, Text> {
	private static int COLUMN_NUM = 0;
	
	private static Map<Long, Integer> TIMES_AVG_MAP = new HashMap<Long, Integer>();
	private static Map<Long, List<Integer>> TIMES_DIS_MAP = new HashMap<Long, List<Integer>>();
	private static Map<Long, Integer> FREQ_AVG_MAP = new HashMap<Long, Integer>();
	private static Map<Long, List<Integer>> FREQ_DIS_MAP = new HashMap<Long, List<Integer>>();

	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		COLUMN_NUM = context.getConfiguration().getInt(
				Common.MALL_SYSTEM_COLUMNS, Common.DEFAULT_COLUMN_NUM);
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		long shopId = Long.parseLong(
				key.toString().split(Common.CTRL_A, -1)[1]);
		
		List<Integer> timesList = new ArrayList<Integer>();
		for (int i = 0; i < COLUMN_NUM; i++) {
			timesList.add(0);
		}
		List<Integer> freqList = new ArrayList<Integer>();
		for (int i = 0; i < COLUMN_NUM; i++) {
			freqList.add(0);
		}
		
		int sumT = 0;
		int sumF = 0;
		int counter = 0;
		for (Text val : values) {
			String arr[] = val.toString().split(Common.CTRL_A, -1);
			int times = Integer.parseInt(arr[0]);
			int freq = Integer.parseInt(arr[1]);
			
			sumT += times;
			sumF += freq;
			counter ++;
			
			int indexTimes = times - 1;
			if (indexTimes < 0) {
				indexTimes = 0;
			}
			if (indexTimes > COLUMN_NUM - 1) {
				indexTimes = COLUMN_NUM - 1;
			}
			timesList.set(indexTimes, timesList.get(indexTimes) + 1);
			
			int indexFreq = (freq - 1) / 2;
			if (indexFreq < 0) {
				indexFreq = 0;
			}
			if (indexFreq > COLUMN_NUM - 1) {
				indexFreq = COLUMN_NUM - 1;
			}
			freqList.set(indexFreq, freqList.get(indexFreq) + 1);
		}
		
		int avgTimes = sumT / counter;
		int avgFreq = sumF /counter;
		TIMES_AVG_MAP.put(shopId, avgTimes);	
		FREQ_AVG_MAP.put(shopId, avgFreq);	
		
		TIMES_DIS_MAP.put(shopId, timesList);
		FREQ_DIS_MAP.put(shopId, freqList);
		
		Text outVal = new Text(avgTimes + Common.CTRL_A + avgFreq);
		context.write(key, outVal);
	}
	
	@Override
	public void cleanup(Context context)
			throws IOException, InterruptedException {
		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		long dateTime = 0;
		try {
			dateTime = timeFormat.parse(
					context.getConfiguration().get(Common.MALL_SYSTEM_TODAY)).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		String mongoServerList = context.getConfiguration().get(
				Common.MONGO_SERVER_LIST);
		String serverArr[] = mongoServerList.split(Common.COMMA, -1);
		if (serverArr.length != Common.MONGO_SERVER_NUM) {
			throw new RuntimeException("Get mongo server fail.");
		}
		String mongoServerFst = serverArr[0];
		String mongoServerSnd = serverArr[1];
		String mongoServerTrd = serverArr[2];

		String mongoDbName = context.getConfiguration().get(
				Common.MONGO_DB_NAME);
		int mongoServerPort = context.getConfiguration().getInt(
				Common.MONGO_SERVER_PORT, Common.DEFAULT_MONGO_PORT);

		MongoClient mongoClient = new MongoClient(Arrays.asList(
				new ServerAddress(mongoServerFst, mongoServerPort),
				new ServerAddress(mongoServerSnd, mongoServerPort),
				new ServerAddress(mongoServerTrd, mongoServerPort)));
		DB mongoDb = mongoClient.getDB(mongoDbName);
		

		String visitCollectionName = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT);
		DBCollection visitCollection = 
				mongoDb.getCollection(visitCollectionName);
		
		String visitKeyShop = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_ID);
		String visitkeyTag = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_TAG);
		String visitCreateTime = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_TIME);
		String visitTimes = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_TIMES);
		String visitTimesDis = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_TIMESDIS);
		String visitFreq = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_FREQ);
		String visitFreqDis = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_FREQDIS);
		
		Iterator<Long> iter = TIMES_DIS_MAP.keySet().iterator();
		while (iter.hasNext()) {
			long shopId = iter.next();
			List<Integer> itemList = TIMES_DIS_MAP.get(shopId);
			int avgDwellTime = TIMES_AVG_MAP.get(shopId);
			
	        BasicDBObject query = new BasicDBObject();
	        query.put(visitKeyShop, shopId);
	        query.put(visitkeyTag, Common.DWELL_TYPE_SHOP);
	        query.put(visitCreateTime, dateTime);
	        
	        BasicDBObject visitDis = new BasicDBObject();
			for (int i = 0; i < itemList.size(); i++) {
				visitDis.put(String.valueOf(i), itemList.get(i));
			}
	        BasicDBObject documentDis = new BasicDBObject();
			documentDis.append(Common.MONGO_OPTION_SET, 
					new BasicDBObject(visitTimesDis, visitDis));
			visitCollection.update(query, documentDis, true, false);
			
	        BasicDBObject document = new BasicDBObject();
			document.append(Common.MONGO_OPTION_SET, 
					new BasicDBObject(visitTimes, avgDwellTime));
			visitCollection.update(query, document, true, false);
		}
		
		iter = FREQ_DIS_MAP.keySet().iterator();
		while (iter.hasNext()) {
			long shopId = iter.next();
			List<Integer> itemList = FREQ_DIS_MAP.get(shopId);
			int avgDwellTime = FREQ_AVG_MAP.get(shopId);
			
	        BasicDBObject query = new BasicDBObject();
	        query.put(visitKeyShop, shopId);
	        query.put(visitkeyTag, Common.DWELL_TYPE_SHOP);
	        query.put(visitCreateTime, dateTime);
	        
	        BasicDBObject visitDis = new BasicDBObject();
			for (int i = 0; i < itemList.size(); i++) {
				visitDis.put(String.valueOf(i), itemList.get(i));
			}
	        BasicDBObject documentDis = new BasicDBObject();
			documentDis.append(Common.MONGO_OPTION_SET, 
					new BasicDBObject(visitFreqDis, visitDis));
			visitCollection.update(query, documentDis, true, false);
			
	        BasicDBObject document = new BasicDBObject();
			document.append(Common.MONGO_OPTION_SET, 
					new BasicDBObject(visitFreq, avgDwellTime));
			visitCollection.update(query, document, true, false);
		}

	
		mongoClient.close();
	}

}
