package com.doodod.mall.statistic;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Flow;
import com.google.protobuf.ByteString;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class FlowShopReducer extends 
	Reducer<Text, BytesWritable, Text, BytesWritable> {
	enum JobCounter {
		MAP_SIZE,
	}
	
	private static int COLUMN_NUM = 0;
	private Map<String, Integer> FLOW_MAP = new HashMap<String, Integer>();
	//private Map<String, Flow> visitMap = new HashMap<String, Flow>();
	private Map<String, Integer> SHOP_AVG_MAP = new HashMap<String, Integer>();
	private Map<String, Integer> ZONE_AVG_MAP = new HashMap<String, Integer>();
	private Map<String, List<Integer>> SHOP_DIS_MAP = new HashMap<String, List<Integer>>();
	private Map<String, List<Integer>> ZONE_DIS_MAP = new HashMap<String, List<Integer>>();
	
	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		COLUMN_NUM = context.getConfiguration().getInt(
				Common.MALL_SYSTEM_COLUMNS, Common.DEFAULT_COLUMN_NUM);
	}

	@Override
	public void reduce(Text key, Iterable<BytesWritable> values, Context context)
			throws IOException, InterruptedException {
		String shopKey = key.toString();
		Flow.Builder fb = Flow.newBuilder();
		
		for (BytesWritable val : values) {
			fb.mergeFrom(val.getBytes(), 0, val.getLength());
		}
		
		Set<ByteString> customerSet = new HashSet<ByteString>();
		for (ByteString mac : fb.getPhoneMacList()) {
			customerSet.add(mac);
		}
		fb.clearPhoneMac();
		for (ByteString mac : customerSet) {
			fb.addPhoneMac(mac);
		}
		
		String keyOut = key.toString() + Common.CTRL_A + String.valueOf(fb.getShopCat());
		FLOW_MAP.put(keyOut, fb.getPhoneMacCount());
		
		if (key.toString().startsWith(Common.FLOW_TAG_DAY)) {
			//visitMap.put(key.toString(), fb.build());
			
			List<Integer> visitShopList = new ArrayList<Integer>();
			for (int i = 0; i < COLUMN_NUM; i++) {
				visitShopList.add(0);
			}
			
			int custNum = fb.getPhoneMacCount();
			int sum = 0;
			for (int time : fb.getShopVisitsList()) {
				int index = (time - 1) / 2;
				if (index > COLUMN_NUM - 1) {
					index = COLUMN_NUM - 1;
				}
				if (index < 0) {
					index = 0;
				}
				visitShopList.set(index, visitShopList.get(index) + 1);
				sum += time;
			}
			int shopAvg = sum / custNum;
			SHOP_AVG_MAP.put(shopKey, shopAvg);
			SHOP_DIS_MAP.put(shopKey, visitShopList);
			
			
			List<Integer> visitZoneList = new ArrayList<Integer>();
			for (int i = 0; i < COLUMN_NUM; i++) {
				visitZoneList.add(0);
			}
			sum = 0;
			for (int time : fb.getZoneVisitsList()) {
				int index = (time - 1) / 2;
				if (index > COLUMN_NUM - 1) {
					index = COLUMN_NUM - 1;
				}
				if (index < 0) {
					index = 0;
				}
				visitZoneList.set(index, visitZoneList.get(index) + 1);
				sum += time;
			}
			int zoneAvg = sum / custNum;
			ZONE_AVG_MAP.put(shopKey, zoneAvg);
			ZONE_DIS_MAP.put(shopKey, visitZoneList);
			
			context.getCounter(JobCounter.MAP_SIZE).increment(1);
		}
		context.write(key, new BytesWritable(fb.build().toByteArray()));
	}
	
	@Override
	public void cleanup(Context context)
			throws IOException, InterruptedException {

		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		long createTime = 0;
		long dateTime = 0;
		try {
			createTime = timeFormat.parse(
					context.getConfiguration().get(Common.MALL_SYSTEM_BIZDATE)).getTime() - 1;
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
		
		
		String flowCollectionName = context.getConfiguration().get(
				Common.MONGO_COLLECTION_STORE);
		DBCollection flowCollection = 
				mongoDb.getCollection(flowCollectionName);
		
		String flowKeyShopId = context.getConfiguration().get(
				Common.MONGO_COLLECTION_STORE_ID);
		String flowKeyShopCat = context.getConfiguration().get(
				Common.MONGO_COLLECTION_STORE_CAT);
		String flowkeyTag = context.getConfiguration().get(
				Common.MONGO_COLLECTION_STORE_TAG);
		String flowCreateTime = context.getConfiguration().get(
				Common.MONGO_COLLECTION_STORE_TIME);
		String flowNum = context.getConfiguration().get(
				Common.MONGO_COLLECTION_STORE_NUMBER);
		String flowKeyMall = context.getConfiguration().get(
				Common.MONGO_COLLECTION_STORE_MALL);
		
		Iterator<String> iter = FLOW_MAP.keySet().iterator();
		while (iter.hasNext()) {
			String key = iter.next();
			int custNum = FLOW_MAP.get(key);

			String arr[] = key.split(Common.CTRL_A, -1);
			int shopId = Integer.parseInt(arr[1]);
			String tag = arr[0];
			int mallId = Integer.parseInt(arr[2]);
			int shopCat = Integer.parseInt(arr[3]);
			
            BasicDBObject document = new BasicDBObject();
            document.put(flowKeyShopId, shopId);
            document.put(flowkeyTag, tag);
            document.put(flowCreateTime, createTime);
            document.put(flowNum, custNum);
            document.put(flowKeyShopCat, shopCat); 
            document.put(flowKeyMall, mallId);
            
            BasicDBObject query = new BasicDBObject();
            query.put(flowKeyShopId, shopId);            
            query.put(flowkeyTag, tag);
            query.put(flowCreateTime, createTime);
            query.put(flowKeyMall, mallId);
            
            //upsert_true: creates a new document when no document matches the query criteria
            //multi_true: updates multiple documents that meet the query criteria.
            flowCollection.update(query, document, true, false);
            
            //flowCollection.insert(document);	
		}
		

		String visitCollectionName = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT);
		DBCollection visitCollection = 
				mongoDb.getCollection(visitCollectionName);
		
		String visitKeyId = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_ID);
		String visitkeyTag = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_TAG);
		String visitCreateTime = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_TIME);
		String visitShop = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_SHOP);
		String visitShopDis = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_SHOPDIS);
		String visitZone = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_ZONE);
		String visitZoneDis = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_ZONEDIS);
		String visitKeyMall = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_SHOPMALL);
		
		Iterator<String> visitIter = SHOP_DIS_MAP.keySet().iterator();
		while (visitIter.hasNext()) {			
			String shopKey = visitIter.next();
			String arr[] = shopKey.split(Common.CTRL_A, -1);
			long shopId = Long.parseLong(arr[1]);
			long mallId = Long.parseLong(arr[2]);

	        BasicDBObject queryShop = new BasicDBObject();
	        queryShop.put(visitKeyId, shopId);
	        queryShop.put(visitkeyTag, Common.DWELL_TYPE_SHOP);
	        queryShop.put(visitCreateTime, dateTime);
	        queryShop.put(visitKeyMall, mallId);

	        List<Integer> visitShopList = SHOP_DIS_MAP.get(shopKey);
	        BasicDBObject visitShopDisObj = new BasicDBObject();
			for (int i = 0; i < visitShopList.size(); i++) {
				visitShopDisObj.put(String.valueOf(i), visitShopList.get(i));
			}
			
	        BasicDBObject documentShopDis = new BasicDBObject();
			documentShopDis.append(Common.MONGO_OPTION_SET, 
					new BasicDBObject(visitShopDis, visitShopDisObj));
			visitCollection.update(queryShop, documentShopDis, true, false);
			
			int shopAvg = SHOP_AVG_MAP.get(shopKey);
	        BasicDBObject documentShop = new BasicDBObject();
			documentShop.append(Common.MONGO_OPTION_SET, 
					new BasicDBObject(visitShop, shopAvg));
			visitCollection.update(queryShop, documentShop, true, false);

			List<Integer> visitZoneList = ZONE_DIS_MAP.get(shopKey);
	        BasicDBObject visitZoneDisObj = new BasicDBObject();
			for (int i = 0; i < visitZoneList.size(); i++) {
				visitZoneDisObj.put(String.valueOf(i), visitZoneList.get(i));
			}
			
	        BasicDBObject documentZoneDis = new BasicDBObject();
	        documentZoneDis.append(Common.MONGO_OPTION_SET, 
					new BasicDBObject(visitZoneDis, visitZoneDisObj));
			visitCollection.update(queryShop, documentZoneDis, true, false);
			
			int zoneAvg = ZONE_AVG_MAP.get(shopKey);
	        BasicDBObject documentZone = new BasicDBObject();
	        documentZone.append(Common.MONGO_OPTION_SET, 
					new BasicDBObject(visitZone, zoneAvg));
			visitCollection.update(queryShop, documentZone, true, false);
		}
		
		mongoClient.close();
		
	}

}
