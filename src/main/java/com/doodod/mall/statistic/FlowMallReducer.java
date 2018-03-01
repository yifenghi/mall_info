package com.doodod.mall.statistic;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Flow;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class FlowMallReducer extends 
	Reducer<Text, BytesWritable, Text, BytesWritable> {
	
	private Map<String, Integer> flowMap = new HashMap<String, Integer>();

	@Override
	public void reduce(Text key, Iterable<BytesWritable> values, Context context)
			throws IOException, InterruptedException {
		Flow.Builder fb = Flow.newBuilder();
		
		for (BytesWritable val : values) {
			fb.mergeFrom(val.getBytes(), 0, val.getLength());
		}
		
		flowMap.put(key.toString(), fb.getPhoneMacCount());
		context.write(key, new BytesWritable(fb.build().toByteArray()));
	}
	
	@Override
	public void cleanup(Context context)
			throws IOException, InterruptedException {

		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		long createTime = 0;
		try {
			createTime = timeFormat.parse(
					context.getConfiguration().get(Common.MALL_SYSTEM_BIZDATE)).getTime() - 1;
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
				Common.MONGO_COLLECTION_MALL);
		DBCollection flowCollection = 
				mongoDb.getCollection(flowCollectionName);
		
		String flowKeyMall = context.getConfiguration().get(
				Common.MONGO_COLLECTION_MALL_ID);
		String flowkeyTag = context.getConfiguration().get(
				Common.MONGO_COLLECTION_MALL_TAG);
		String flowCreateTime = context.getConfiguration().get(
				Common.MONGO_COLLECTION_MALL_TIME);
		String flowNum = context.getConfiguration().get(
				Common.MONGO_COLLECTION_MALL_NUMBER);
		
		Iterator<String> iter = flowMap.keySet().iterator();
		while (iter.hasNext()) {
			String key = iter.next();			
			int custNum = flowMap.get(key);

			String arr[] = key.split(Common.CTRL_A, -1);
			long mallId = Long.parseLong(arr[1]);
			String tag = arr[0];
			
            BasicDBObject document = new BasicDBObject();
            document.put(flowKeyMall, mallId);            
            document.put(flowkeyTag, tag);
            document.put(flowCreateTime, createTime);
            document.put(flowNum, custNum);

            BasicDBObject query = new BasicDBObject();
            query.put(flowKeyMall, mallId);            
            query.put(flowkeyTag, tag);
            query.put(flowCreateTime, createTime);
            
            flowCollection.update(query, document, true, false);
       }
		
		mongoClient.close();
	}

}
