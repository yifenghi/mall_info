package com.doodod.mall.statistic;

import java.io.IOException;
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

public class FlowMallShopReducer extends Reducer<Text, Text, Text, Text> {
	private static int COLUMN_NUM;
	private static int MALL_ID;
	private static int AVG_SHOP_VISIT;
	private static int AVG_ZONE_VISIT;
	private static List<Integer> SHOP_LIST = new ArrayList<Integer>();
	private static List<Integer> ZONE_LIST = new ArrayList<Integer>();

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		COLUMN_NUM = context.getConfiguration().getInt(
				Common.MALL_SYSTEM_COLUMNS, Common.DEFAULT_COLUMN_NUM);

		for (int i = 0; i < COLUMN_NUM; i++) {
			SHOP_LIST.add(0);
		}

		for (int i = 0; i < COLUMN_NUM; i++) {
			ZONE_LIST.add(0);
		}
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int sumShop = 0;
		int sumzone = 0;
		int custNum = 0;
		for (Text value : values) {
			String arr[] = value.toString().split(Common.CTRL_A, -1);
			int shopNum = Integer.parseInt(arr[0]);
			int zoneNum = Integer.parseInt(arr[1]);

			sumShop += shopNum;
			sumzone += zoneNum;

			int shopIndex = (shopNum - 1) / 2;
			if (shopIndex < 0) {
				shopIndex = 0;
			}
			if (shopIndex > COLUMN_NUM - 1) {
				shopIndex = COLUMN_NUM - 1;
			}
			SHOP_LIST.set(shopIndex, SHOP_LIST.get(shopIndex) + 1);

			int zoneIndex = (zoneNum - 1) / 2;
			if (zoneIndex < 0) {
				zoneIndex = 0;
			}
			if (zoneIndex > COLUMN_NUM - 1) {
				zoneIndex = COLUMN_NUM - 1;
			}
			ZONE_LIST.set(zoneIndex, ZONE_LIST.get(zoneIndex) + 1);
			custNum++;
		}

		MALL_ID = Integer.parseInt(key.toString());

		AVG_SHOP_VISIT = sumShop / custNum;
		AVG_ZONE_VISIT = sumzone / custNum;

		context.write(key, new Text(AVG_SHOP_VISIT + Common.CTRL_A
				+ AVG_ZONE_VISIT));
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {

		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		long dateTime = 0;
		try {
			dateTime = timeFormat.parse(
					context.getConfiguration().get(Common.MALL_SYSTEM_TODAY))
					.getTime();
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
		DBCollection visitCollection = mongoDb
				.getCollection(visitCollectionName);

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

		BasicDBObject query = new BasicDBObject();
		query.put(visitKeyId, MALL_ID);
		query.put(visitkeyTag, Common.DWELL_TYPE_MALL);
		query.put(visitCreateTime, dateTime);

		BasicDBObject shopDis = new BasicDBObject();
		for (int i = 0; i < SHOP_LIST.size(); i++) {
			shopDis.put(String.valueOf(i), SHOP_LIST.get(i));
		}
		BasicDBObject documentShopDis = new BasicDBObject();
		documentShopDis.append(Common.MONGO_OPTION_SET, 
				new BasicDBObject(visitShopDis, shopDis));
		visitCollection.update(query, documentShopDis, true, false);
		
		BasicDBObject zoneDis = new BasicDBObject();
		for (int i = 0; i < ZONE_LIST.size(); i++) {
			zoneDis.put(String.valueOf(i), ZONE_LIST.get(i));
		}
		BasicDBObject documentzoneDis = new BasicDBObject();
		documentzoneDis.append(Common.MONGO_OPTION_SET, 
				new BasicDBObject(visitZoneDis, zoneDis));
		visitCollection.update(query, documentzoneDis, true, false);
		
		BasicDBObject documentShop = new BasicDBObject();
		documentShop.append(Common.MONGO_OPTION_SET, 
				new BasicDBObject(visitShop, AVG_SHOP_VISIT));
		visitCollection.update(query, documentShop, true, false);
		
		BasicDBObject documentzone = new BasicDBObject();
		documentzone.append(Common.MONGO_OPTION_SET, 
				new BasicDBObject(visitZone, AVG_ZONE_VISIT));
		visitCollection.update(query, documentzone, true, false);
		
		mongoClient.close();
	}

}
