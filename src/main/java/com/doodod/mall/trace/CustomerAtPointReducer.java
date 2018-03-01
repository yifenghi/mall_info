package com.doodod.mall.trace;

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
import com.doodod.mall.common.Coordinates;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class CustomerAtPointReducer extends
	Reducer<Text, LongWritable, Text, LongWritable> {

	private static Map<Coordinates, Integer> LOCATION_MAP = new HashMap<Coordinates, Integer>();
	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		int counter = 0;
		for (LongWritable val : values) {
			counter += val.get();
		}
		
		String arr[] = key.toString().split(Common.CTRL_A, -1);
		long floorId = Long.parseLong(arr[0]);
		double x = Double.parseDouble(arr[1]);
		double y = Double.parseDouble(arr[2]);
		
		Coordinates coor = new Coordinates(x, y, floorId);
		LOCATION_MAP.put(coor, counter);
		context.write(key, new LongWritable(counter));
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
		

		String locCollectionName = context.getConfiguration().get(
				Common.MONGO_COLLECTION_LOCATION);
		DBCollection locCollection = 
				mongoDb.getCollection(locCollectionName);
		
		String locKeyFloor = context.getConfiguration().get(
				Common.MONGO_COLLECTION_LOCATION_ID);
		String locKeyX = context.getConfiguration().get(
				Common.MONGO_COLLECTION_LOCATION_X);
		String locKeyY = context.getConfiguration().get(
				Common.MONGO_COLLECTION_LOCATION_Y);
		String locCreateTime = context.getConfiguration().get(
				Common.MONGO_COLLECTION_LOCATION_TIME);
		String locNum = context.getConfiguration().get(
				Common.MONGO_COLLECTION_LOCATION_NUM);
		
		Iterator<Coordinates> iter = LOCATION_MAP.keySet().iterator();
		while (iter.hasNext()) {
			Coordinates coor = iter.next();
			
			BasicDBObject query = new BasicDBObject();
			query.put(locKeyFloor, coor.getZ());
			query.put(locKeyX, coor.getX());
			query.put(locKeyY, coor.getY());
			query.put(locCreateTime, createTime);
			
			BasicDBObject document = new BasicDBObject();
			document.put(locKeyFloor, coor.getZ());
			document.put(locKeyX, coor.getX());
			document.put(locKeyY, coor.getY());
			document.put(locCreateTime, createTime);
			document.put(locNum, LOCATION_MAP.get(coor));
			
			locCollection.update(query, document, true, false);
		}
		mongoClient.close();

	}
}
