/**
 * @author yupeng.cao@palmaplus.com
 */
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


public class FlowFloorReducer extends 
	Reducer<Text, BytesWritable, Text, BytesWritable> {
	enum JobCounter {
	}
	
	private Map<String, Integer> flowMap = new HashMap<String, Integer>();
	private Map<String, Flow> visitMap = new HashMap<String, Flow>();

	@Override
	public void reduce(Text key, Iterable<BytesWritable> values, Context context)
			throws IOException, InterruptedException {
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
		
		flowMap.put(key.toString(), fb.getPhoneMacCount());
		if (key.toString().startsWith(Common.FLOW_TAG_DAY)) {
			visitMap.put(key.toString(), fb.build());
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
				Common.MONGO_COLLECTION_FLOOR);
		DBCollection flowCollection = 
				mongoDb.getCollection(flowCollectionName);
		
		String flowKeyFloor = context.getConfiguration().get(
				Common.MONGO_COLLECTION_FlOOR_ID);
		String flowkeyTag = context.getConfiguration().get(
				Common.MONGO_COLLECTION_FlOOR_TAG);
		String flowCreateTime = context.getConfiguration().get(
				Common.MONGO_COLLECTION_FlOOR_TIME);
		String flowNum = context.getConfiguration().get(
				Common.MONGO_COLLECTION_FlOOR_NUMBER);
		
		Iterator<String> iter = flowMap.keySet().iterator();
		while (iter.hasNext()) {
			String key = iter.next();			
			int custNum = flowMap.get(key);

			String arr[] = key.split(Common.CTRL_A, -1);
			int planarGraph = Integer.parseInt(arr[1]);
			String tag = arr[0];
			
            BasicDBObject document = new BasicDBObject();
            document.put(flowKeyFloor, planarGraph);            
            document.put(flowkeyTag, tag);
            document.put(flowCreateTime, createTime);
            document.put(flowNum, custNum);

            
            BasicDBObject query = new BasicDBObject();
            query.put(flowKeyFloor, planarGraph);            
            query.put(flowkeyTag, tag);
            query.put(flowCreateTime, createTime);
            
            //upsert_true: creates a new document when no document matches the query criteria
            //multi_true: updates multiple documents that meet the query criteria.
            flowCollection.update(query, document, true, false);
            //flowCollection.insert(document);	
		}
		
		
		int floorNum = context.getConfiguration().getInt(
				Common.MALL_SYSTEM_FLOORS, Common.DEFAULT_FLOOR_NUM);
		String visitCollectionName = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT);
		DBCollection visitCollection = 
				mongoDb.getCollection(visitCollectionName);
		
		String visitKeyFloor = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_ID);
		String visitkeyTag = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_TAG);
		String visitCreateTime = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_TIME);
		String visitFloor = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_FLOOR);
		String visitFloorDis = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_FLOORDIS);
		
		iter = visitMap.keySet().iterator();
		while (iter.hasNext()) {			
			String key = iter.next();			
			long floorId = Long.parseLong(
					key.split(Common.CTRL_A, -1)[1]);
			
			Flow flow = visitMap.get(key);
			
	        BasicDBObject query = new BasicDBObject();
	        query.put(visitKeyFloor, floorId);
	        query.put(visitkeyTag, Common.DWELL_TYPE_FLOOR);
	        query.put(visitCreateTime, dateTime);


	        BasicDBObject visitDis = new BasicDBObject();
			List<Integer> visitList = new ArrayList<Integer>();
			for (int i = 0; i < floorNum; i++) {
				visitList.add(0);
			}
			
			int custNum = flow.getPhoneMacCount();
			int sum = 0;
			for (int time : flow.getFloorVisitsList()) {
				visitList.set(time - 1, visitList.get(time - 1) + 1);
				sum += time;
			}
			
			for (int i = 0; i < visitList.size(); i++) {
				visitDis.put(String.valueOf(i), visitList.get(i));
			}
			
			double avg = sum / (1.0 * custNum);			
	        BasicDBObject documentDis = new BasicDBObject();
	        documentDis.append(Common.MONGO_OPTION_SET, 
					new BasicDBObject(visitFloorDis, visitDis));
			visitCollection.update(query, documentDis, true, false);

	        BasicDBObject document = new BasicDBObject();
			document.append(Common.MONGO_OPTION_SET, 
					new BasicDBObject(visitFloor, avg));	
			visitCollection.update(query, document, true, false);
		}
		
		mongoClient.close();
		
	}

}
