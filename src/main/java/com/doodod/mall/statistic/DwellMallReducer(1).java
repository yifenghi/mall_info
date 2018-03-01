package com.doodod.mall.statistic;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.doodod.mall.common.Common;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class DwellMallReducer extends
	Reducer<Text, Text, Text, Text> {
	enum JobCounter {
		TEST
	}
	private static int COLUMN_NUM = 0;
	private static int FLOOR_NUM = 0;
	private static long MALL_ID = 0;
	
	private static double AVG_FLOOR_NUM = 0;
	private static int AVG_DWELL_NUM = 0;
	private static List<Integer> FLOOR_LIST = new ArrayList<Integer>();
	private static List<Integer> DWELL_LIST = new ArrayList<Integer>();
	
	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		COLUMN_NUM = context.getConfiguration().getInt(
				Common.MALL_SYSTEM_COLUMNS, Common.DEFAULT_COLUMN_NUM);
		FLOOR_NUM = context.getConfiguration().getInt(
				Common.MALL_SYSTEM_FLOORS, Common.DEFAULT_FLOOR_NUM);
		
		for (int i = 0; i < FLOOR_NUM; i++) {
			FLOOR_LIST.add(0);
		}
		for (int i = 0; i < COLUMN_NUM; i++) {
			DWELL_LIST.add(0);
		}
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int counter = 0;
		int sumFloors = 0;
		int sumDwell = 0;
		MALL_ID = Long.parseLong(key.toString());
		
		for (Text val : values) {
			String arr[] = val.toString().split(Common.CTRL_A, -1);
			int floorNum = Integer.parseInt(arr[0]);
			int dwellNum = Integer.parseInt(arr[1]);
			
			sumFloors += floorNum;
			sumDwell += dwellNum;
			counter ++;
			
			int floorIndex = floorNum - 1;
			FLOOR_LIST.set(floorIndex, FLOOR_LIST.get(floorIndex) + 1);
			
			int dwellIndex = (dwellNum - 1) / 5;
			if (dwellIndex < 0) {
				dwellIndex = 0;
			}
			if (dwellIndex > COLUMN_NUM - 1) {
				dwellIndex = COLUMN_NUM - 1;
			}
			DWELL_LIST.set(dwellIndex, DWELL_LIST.get(dwellIndex) + 1);
				
		}
		
		AVG_FLOOR_NUM = sumFloors / (1.0 * counter);
		AVG_DWELL_NUM = sumDwell /counter;
		
		DecimalFormat format = new DecimalFormat(Common.NUM_FORNAT);
		Text outVal = new Text(format.format(AVG_FLOOR_NUM) + Common.CTRL_A + AVG_DWELL_NUM);
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
		
		String visitKeyMall = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_ID);
		String visitkeyTag = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_TAG);
		String visitCreateTime = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_TIME);
		String visitDwell = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_DWELL);
		String visitDwellDis = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_DWELLDIS);
		String visitFloor = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_FLOOR);
		String visitFloorDis = context.getConfiguration().get(
				Common.MONGO_COLLECTION_VISIT_FLOORDIS);
		
        BasicDBObject query = new BasicDBObject();
        query.put(visitKeyMall, MALL_ID);
        query.put(visitkeyTag, Common.DWELL_TYPE_MALL);
        query.put(visitCreateTime, dateTime);
        
        BasicDBObject floorDis = new BasicDBObject();
		for (int i = 0; i < FLOOR_LIST.size(); i++) {
			floorDis.put(String.valueOf(i), FLOOR_LIST.get(i));
		}
        BasicDBObject documentFloorDis = new BasicDBObject();
        documentFloorDis.append(Common.MONGO_OPTION_SET, 
				new BasicDBObject(visitFloorDis, floorDis));
		visitCollection.update(query, documentFloorDis, true, false);
		
        BasicDBObject dwellDis = new BasicDBObject();
		for (int i = 0; i < DWELL_LIST.size(); i++) {
			dwellDis.put(String.valueOf(i), DWELL_LIST.get(i));
		}
		BasicDBObject documentDwellDis = new BasicDBObject();
		documentDwellDis.append(Common.MONGO_OPTION_SET, 
				new BasicDBObject(visitDwellDis, dwellDis));
		visitCollection.update(query, documentDwellDis, true, false);
		
        BasicDBObject documnetFloor = new BasicDBObject();
        documnetFloor.append(Common.MONGO_OPTION_SET, 
        		new BasicDBObject(visitFloor, AVG_FLOOR_NUM));
        visitCollection.update(query, documnetFloor, true, false);
        
        BasicDBObject documnetDwell = new BasicDBObject();
        documnetDwell.append(Common.MONGO_OPTION_SET, 
        		new BasicDBObject(visitDwell, AVG_DWELL_NUM));
        visitCollection.update(query, documnetDwell, true, false);
		
		mongoClient.close();
	}

}
