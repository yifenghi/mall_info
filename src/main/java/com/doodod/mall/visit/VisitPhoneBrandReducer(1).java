package com.doodod.mall.visit;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
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

public class VisitPhoneBrandReducer extends 
	Reducer<Text, LongWritable, Text, LongWritable> {
	private static Map<String, Long> BRAND_COUNT_MAP = new HashMap<String, Long>();

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		long sum = 0;
		for (LongWritable value : values) {
			sum += value.get();
		}
		
		BRAND_COUNT_MAP.put(key.toString(), sum);
		context.write(key, new LongWritable(sum));
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

		String brandCollectionName = context.getConfiguration().get(
				Common.MONGO_COLLECTION_BRAND);
		DBCollection brandCollection = 
				mongoDb.getCollection(brandCollectionName);
		
		String brandName = context.getConfiguration().get(
				Common.MONGO_COLLECTION_BRAND_NAME);
		String brandCount = context.getConfiguration().get(
				Common.MONGO_COLLECTION_BRAND_COUNT);
		String createTime = context.getConfiguration().get(
				Common.MONGO_COLLECTION_BRAND_TIME);
		String brandMallId = context.getConfiguration().get(
				Common.MONGO_COLLECTION_BRAND_MALL);
		
		Iterator<String> iter = BRAND_COUNT_MAP.keySet().iterator();
		while (iter.hasNext()) {
			String keyName = iter.next();
			String arr[] = keyName.split(Common.CTRL_A, -1);
			String name = arr[0];
			int mallId = Integer.parseInt(arr[1]);
			
			long count = BRAND_COUNT_MAP.get(keyName);
			
	        BasicDBObject query = new BasicDBObject();
	        query.put(brandName, name);
	        query.put(createTime, dateTime);
	        query.put(brandMallId, mallId);
	        
	        BasicDBObject document = new BasicDBObject();
	        document.put(brandName, name);
	        document.put(createTime, dateTime);
	        document.put(brandCount, count);
	        document.put(brandMallId, mallId);
	        
	        brandCollection.update(query, document, true, false);	
		}
		mongoClient.close();	
	}
	
}
