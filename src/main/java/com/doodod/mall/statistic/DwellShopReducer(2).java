package com.doodod.mall.statistic;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.doodod.mall.common.Common;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class DwellShopReducer extends 
	Reducer<Text, Text, Text, LongWritable> {
	private static int COLUMN_NUM = 0;
	private static Map<String, Integer> AVG_DWELL_MAP = new HashMap<String, Integer>();
	private static Map<String, List<Integer>> DWELL_MAP = new HashMap<String, List<Integer>>();
	
	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		COLUMN_NUM = context.getConfiguration().getInt(
				Common.MALL_SYSTEM_COLUMNS, Common.DEFAULT_COLUMN_NUM);
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> vals, Context context)
			throws IOException, InterruptedException {
		List<Integer> dwellList = new ArrayList<Integer>();
		int timeAvg = 0;
		String shopKey = key.toString();
		
		int customerCounter = 0;
		long timeTotal = 0;
		
		for (int i = 0; i < COLUMN_NUM; i++) {
			dwellList.add(0);
		}
		
		for (Text val : vals) {
			long time = Long.parseLong(
					val.toString().split(Common.CTRL_A, -1)[1]);
			int index = (int) (time - 1) / 5;
			if (index < 0) {
				index = 0;
			}
			if (index > COLUMN_NUM - 1) {
				index = COLUMN_NUM - 1;
			}
			
			dwellList.set(index, dwellList.get(index) + 1);
			timeTotal += time;
			customerCounter ++;
		}
		

		if (customerCounter > 0) {
			timeAvg = (int) timeTotal / customerCounter;
		}
		
		context.write(key, new LongWritable(timeAvg));
		DWELL_MAP.put(shopKey, dwellList);
		AVG_DWELL_MAP.put(shopKey, timeAvg);
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
		String visitDwell = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_DWELL);
		String visitDwellDis = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_DWELLDIS);
		String visitKeyMall = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_SHOPMALL);
		
		Iterator<String> iter = DWELL_MAP.keySet().iterator();
		while (iter.hasNext()) {
			String shopKey = iter.next();
			String arr[] = shopKey.split(Common.CTRL_A, -1);
			long shopId = Long.parseLong(arr[1]);
			long mallId = Long.parseLong(arr[2]);
			
			List<Integer> dwellList = DWELL_MAP.get(shopKey);
			int avgDwellTime = AVG_DWELL_MAP.get(shopKey);
			
	        BasicDBObject query = new BasicDBObject();
	        query.put(visitKeyShop, shopId);
	        query.put(visitkeyTag, Common.DWELL_TYPE_SHOP);
	        query.put(visitCreateTime, dateTime);
	        query.put(visitKeyMall, mallId);
	        
	        BasicDBObject visitDis = new BasicDBObject();
			for (int i = 0; i < dwellList.size(); i++) {
				visitDis.put(String.valueOf(i), dwellList.get(i));
			}
	        BasicDBObject documentDis = new BasicDBObject();
			documentDis.append(Common.MONGO_OPTION_SET, 
					new BasicDBObject(visitDwellDis, visitDis));
			visitCollection.update(query, documentDis, true, false);
			
	        BasicDBObject document = new BasicDBObject();
			document.append(Common.MONGO_OPTION_SET, 
					new BasicDBObject(visitDwell, avgDwellTime));
			visitCollection.update(query, document, true, false);
		}

	
		mongoClient.close();
	}
}
