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

public class VisitShopInfoReducer extends 
	Reducer<Text, Text, Text, Text> {
	private static int COLUMN_NUM = 0;
	private static int TYPE_TAG = 0;
	
	private static Map<String, Integer> TIMES_AVG_MAP = new HashMap<String, Integer>();
	private static Map<String, List<Integer>> TIMES_DIS_MAP = new HashMap<String, List<Integer>>();
	private static Map<String, Integer> FREQ_AVG_MAP = new HashMap<String, Integer>();
	private static Map<String, List<Integer>> FREQ_DIS_MAP = new HashMap<String, List<Integer>>();
	private static Map<String, Integer> NEW_CUSTOMER_MAP = new HashMap<String, Integer>();
	private static Map<String, Integer> OLD_CUSTOMER_MAP = new HashMap<String, Integer>();

	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		COLUMN_NUM = context.getConfiguration().getInt(
				Common.MALL_SYSTEM_COLUMNS, Common.DEFAULT_COLUMN_NUM);
		TYPE_TAG = context.getConfiguration().getInt(
				Common.MONGO_COLLECTION_VISIT_TAGTYPE, Common.DEFAULT_TAG_TYPE);
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		String shopKey = key.toString();
		
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
		int sumNewCust = 0;
		int sumOldCust = 0;
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
			
			int indexFreq = 0;
			if (freq > 0) {
				indexFreq = (freq - 1) / 2 + 1;
			}
			if (indexFreq > COLUMN_NUM - 1) {
				indexFreq = COLUMN_NUM - 1;
			}
			freqList.set(indexFreq, freqList.get(indexFreq) + 1);
			
			sumNewCust += Integer.parseInt(arr[2]);
			sumOldCust += Integer.parseInt(arr[3]);
		}
		
		int avgTimes = sumT / counter;
		int avgFreq = sumF /counter;
		TIMES_AVG_MAP.put(shopKey, avgTimes);	
		FREQ_AVG_MAP.put(shopKey, avgFreq);	
		
		TIMES_DIS_MAP.put(shopKey, timesList);
		FREQ_DIS_MAP.put(shopKey, freqList);
		
		NEW_CUSTOMER_MAP.put(shopKey, sumNewCust);
		OLD_CUSTOMER_MAP.put(shopKey, sumOldCust);
		
		Text outKey = new Text(TYPE_TAG + Common.CTRL_A + shopKey);
		Text outVal = new Text(avgTimes + Common.CTRL_A + avgFreq
				+ Common.CTRL_A + sumNewCust + Common.CTRL_A + sumOldCust);
		context.write(outKey, outVal);
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
		
		String visitKey = context.getConfiguration().get(
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
		String visitNewCust = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_NEWCUST);
		String visitOldCust = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_OLDCUST);
		String visitKeyMall = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_SHOPMALL);
		
		
		Iterator<String> iter = TIMES_DIS_MAP.keySet().iterator();
		while (iter.hasNext()) {
			String shopKey = iter.next();
			String arr[] = shopKey.split(Common.CTRL_A, -1);
			long id = Long.parseLong(arr[1]);
			long mallId = Long.parseLong(arr[2]);
			
 			List<Integer> itemList = TIMES_DIS_MAP.get(shopKey);
			int avgDwellTime = TIMES_AVG_MAP.get(shopKey);
			
	        BasicDBObject query = new BasicDBObject();
	        query.put(visitKey, id);
	        query.put(visitkeyTag, TYPE_TAG);
	        query.put(visitCreateTime, dateTime);
	        query.put(visitKeyMall, mallId);
	        
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
			
			int newCustNum = NEW_CUSTOMER_MAP.get(shopKey);
			BasicDBObject docNewCust = new BasicDBObject();
			docNewCust.append(Common.MONGO_OPTION_SET, 
					new BasicDBObject(visitNewCust, newCustNum));
			visitCollection.update(query, docNewCust, true, false);

			int oldCustNum = OLD_CUSTOMER_MAP.get(shopKey);
			BasicDBObject docOldCust = new BasicDBObject();
			docOldCust.append(Common.MONGO_OPTION_SET, 
					new BasicDBObject(visitOldCust, oldCustNum));
			visitCollection.update(query, docOldCust, true, false);
		}
		
		iter = FREQ_DIS_MAP.keySet().iterator();
		while (iter.hasNext()) {
			String shopKey = iter.next();
			String arr[] = shopKey.split(Common.CTRL_A, -1);
			long id = Long.parseLong(arr[1]);
			long mallId = Long.parseLong(arr[2]);
			
			List<Integer> itemList = FREQ_DIS_MAP.get(shopKey);
			int avgDwellTime = FREQ_AVG_MAP.get(shopKey);
			
	        BasicDBObject query = new BasicDBObject();
	        query.put(visitKey, id);
	        query.put(visitkeyTag, TYPE_TAG);
	        query.put(visitCreateTime, dateTime);
	        query.put(visitKeyMall, mallId);
	        
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
