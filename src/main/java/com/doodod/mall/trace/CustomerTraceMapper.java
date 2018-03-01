package com.doodod.mall.trace;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class CustomerTraceMapper extends 
	Mapper<Text, BytesWritable, Text, Text> {
	enum JobCounter {
		CUSTOMER_MAP_OK,
		CUSTOMER_DB_OK,
	}
	
	public static String TRACE_X = "";
	public static String TRACE_Y = "";
	public static String TRACE_Z = "";
	
	public static Set<String> CUSTOMER_SET = new HashSet<String>();
	public static Map<String, List<BasicDBObject>> CUSTOMER_MAP = new HashMap<String, List<BasicDBObject>>();
	
	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		Charset charSet = Charset.forName("UTF-8");
		String customerPath = context.getConfiguration().get(Common.CUSTOMER_CONF_VIP);
		BufferedReader customerReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(customerPath), charSet));
		String line = "";
		while ((line = customerReader.readLine()) != null) {
			CUSTOMER_SET.add(line.trim());
		} 
		customerReader.close();
		
		TRACE_X = context.getConfiguration().get(Common.MONGO_COLLECTION_TRACE_X);
		TRACE_Y = context.getConfiguration().get(Common.MONGO_COLLECTION_TRACE_Y);
		TRACE_Z = context.getConfiguration().get(Common.MONGO_COLLECTION_TRACE_Z);
	}
	
	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		
		String custMac = new String(cb.getPhoneMac().toByteArray());
		if (!CUSTOMER_SET.contains(custMac)) {
			return;
		}
		
		List<Location> locationList = new ArrayList<Location>();
		for (Location loc : cb.getLocationList()) {
			locationList.add(loc);

		}
		LocationComparator locCmp = new LocationComparator();
		Collections.sort(locationList, locCmp);
		
		List<BasicDBObject> coorList = new ArrayList<BasicDBObject>();
		for (Location loc : locationList) {
			BasicDBObject docCoor = new BasicDBObject();
			docCoor.put(TRACE_X, loc.getLocationX());
			docCoor.put(TRACE_Y, loc.getLocationY());
			docCoor.put(TRACE_Z, loc.getPlanarGraph());
			coorList.add(docCoor);
		}
		CUSTOMER_MAP.put(custMac, coorList);
		
		context.getCounter(JobCounter.CUSTOMER_MAP_OK).increment(1);
		context.write(key, new Text());		
	}
	
	public class LocationComparator implements Comparator<Location> {
		public int compare(Location loc1, Location loc2) {
			int locSize1 = loc1.getTimeStampCount();
			int locSize2 = loc2.getTimeStampCount();
			int res = (loc1.getTimeStamp(locSize1 - 1)
						> loc2.getTimeStamp(locSize2 - 1)) ? -1 : 1;	
			return res;
		}
	}
	
	@Override
	public void cleanup(Context context)
			throws IOException, InterruptedException {
		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		long createTime = 0;
		try {
			createTime = timeFormat.parse(
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

		String traceCollectionName = context.getConfiguration().get(
				Common.MONGO_COLLECTION_TRACE);
		DBCollection traceCollection = mongoDb
				.getCollection(traceCollectionName);

		String traceKeyId = context.getConfiguration().get(
				Common.MONGO_COLLECTION_TRACE_ID);
		String tracekeyList = context.getConfiguration().get(
				Common.MONGO_COLLECTION_TRACE_LIST);
		String traceCreateTime = context.getConfiguration().get(
				Common.MONGO_COLLECTION_TRACE_TIME);

		Iterator<String> iter = CUSTOMER_MAP.keySet().iterator();
		while (iter.hasNext()) {
			String userMac = iter.next();
			List<BasicDBObject> docList = CUSTOMER_MAP.get(userMac);

			BasicDBObject document = new BasicDBObject();
			document.put(traceKeyId, userMac);
			document.put(traceCreateTime, createTime);
			document.put(tracekeyList, docList);

			BasicDBObject query = new BasicDBObject();
			query.put(traceKeyId, userMac);
			query.put(traceCreateTime, createTime);

			traceCollection.update(query, document, true, false);
			context.getCounter(JobCounter.CUSTOMER_DB_OK).increment(1);
		}
		mongoClient.close();
	}
}
