/**
 * @author yupeng.cao@palmaplus.com
 */
package com.doodod.mall.statistic;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
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
import com.doodod.mall.common.Coordinates;
import com.doodod.mall.common.Polygon;
import com.doodod.mall.common.TimeCluster;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;
import com.doodod.mall.message.Mall.UserType;
import com.doodod.mall.message.Mall.Visit;
import com.doodod.mall.statistic.ShopMapper.JobCounter;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class MergeReducer extends 
	Reducer<Text, BytesWritable, Text, BytesWritable> {
	enum JobCounter {
		TOTAL_REDUCE_OK,
		PART_REDUCE_OK,
		INVALID_RECORD,
		TOATL_IS_EMPTY,
		PART_IS_EMPTY,
		PART_COORDINATE_CONFLIT,
		PART_NEW,
		PART_IN_OLD,
		PASSENGER_NUM,
		LOCATION_ZERO,
		STORE_FROMAT_ERROR,
		STORE_COOR_ERROR,
		PASSNEGER_OUTOF_FRAME,
		MALL_ID_ERROR,
	}
	
	private static double DWELL_PERCENT_FILTER = 0;
	
	private static int PASSENGER_DWELL_FILTER = Common.DEFAULT_PASSENGER_FILTER;
	private static int FLOOR_NUMBER_FILTER = Common.DEFAULT_PASSENGER_FILTER;
	private static int LOCATION_NUMBER_FILTER = Common.DEFAULT_PASSENGER_FILTER;

	private static Map<Integer, Set<String>> MACHINE_MAP = new HashMap<Integer, Set<String>>(); 
	private static Map<Integer, Set<String>> EMPLOYEE_MAP = new HashMap<Integer, Set<String>>(); 
	private static Map<Long, List<List<Coordinates>>> FRAME_MAP = new HashMap<Long, List<List<Coordinates>>>();
	private static Map<Long, Integer> FLOOR_MALL_MAP = new HashMap<Long, Integer>();

	public void setup(Context context) 
			throws IOException ,InterruptedException {
		Charset charSet = Charset.forName("UTF-8");
		String machinePath = context.getConfiguration().get(Common.CONF_MACHINE_LIST);
		BufferedReader machineReader = new BufferedReader(
				new InputStreamReader(new FileInputStream(machinePath), charSet));
		String line = "";
		while ((line = machineReader.readLine()) != null) {
			String arr[] = line.split(Common.CTRL_A, -1);
			int mallId = Integer.parseInt(arr[0]);
			String mac = arr[1].trim();
			
			if (MACHINE_MAP.containsKey(mallId)) {
				MACHINE_MAP.get(mallId).add(mac);
			}
			else {
				Set<String> macSet = new HashSet<String>();
				macSet.add(mac);
				MACHINE_MAP.put(mallId, macSet);
			}
		}
		machineReader.close();
		
		String employeePath = context.getConfiguration().get(Common.CONF_EMPLOYEE_LIST);
		BufferedReader employeeReader = new BufferedReader(
				new InputStreamReader(new FileInputStream(employeePath), charSet));
		while ((line = employeeReader.readLine()) != null) {
			String arr[] = line.split(Common.CTRL_A, -1);
			int mallId = Integer.parseInt(arr[0]);
			String mac = arr[1].trim();
			
			if (EMPLOYEE_MAP.containsKey(mallId)) {
				EMPLOYEE_MAP.get(mallId).add(mac);
			}
			else {
				Set<String> macSet = new HashSet<String>();
				macSet.add(mac);
				EMPLOYEE_MAP.put(mallId, macSet);
			}
		}
		employeeReader.close();
		
		String framePath = context.getConfiguration().get(Common.SHOP_CONF_FRAME);
		BufferedReader frameReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(framePath), charSet));
		line = "";
		while ((line = frameReader.readLine()) != null) {
			String[] arrFrame = line.split(Common.CTRL_A, -1);
			if (arrFrame.length != 4) {
				context.getCounter(JobCounter.STORE_FROMAT_ERROR).increment(1);		
				continue;
			}
			long floor = Long.parseLong(arrFrame[0]);
			
			String coorList[] = arrFrame[3].split(Common.CTRL_B, -1);
			List<Coordinates> coordinatesList = new ArrayList<Coordinates>();
			for (int i = 0; i < coorList.length; i++) {
				String coorArr[] = coorList[i].split(Common.CTRL_C, -1);
				if (coorArr.length != 2) {
					context.getCounter(JobCounter.STORE_COOR_ERROR).increment(1);
					continue;
				}
				double x = Double.parseDouble(coorArr[0]);
				double y = Double.parseDouble(coorArr[1]);
				
				Coordinates coordinate = new Coordinates(x, y, floor);
				coordinatesList.add(coordinate);
			}
			
			if (FRAME_MAP.containsKey(floor)) {
				FRAME_MAP.get(floor).add(coordinatesList);
			}
			else {
				List<List<Coordinates>> list = new ArrayList<List<Coordinates>>();
				list.add(coordinatesList);
				FRAME_MAP.put(floor, list);
			}
		}
		frameReader.close();
		
		String mallIdPath = context.getConfiguration().get(Common.CONF_FLOORMALL_MAP);
		BufferedReader mallIdReader = new BufferedReader(
				new InputStreamReader(new FileInputStream(mallIdPath), charSet));
		line = "";
		while ((line = mallIdReader.readLine()) != null) {
			String arr[] = line.split(Common.CTRL_A, -1);
			if (arr.length < Common.FILE_MALL_FLOOR_SIZE) {
				continue;
			}
			int mallId = Integer.parseInt(arr[0]);
			long floorId = Long.parseLong(arr[1]);
			
			FLOOR_MALL_MAP.put(floorId, mallId);
		}
		mallIdReader.close();
		
		PASSENGER_DWELL_FILTER = context.getConfiguration().getInt(
				Common.CONF_PASSENGER_FILTER,  Common.DEFAULT_PASSENGER_FILTER);
		FLOOR_NUMBER_FILTER = context.getConfiguration().getInt(
				Common.CONF_FLOOR_FILTER, Common.DEFAULT_FLOOR_FILTER);
		LOCATION_NUMBER_FILTER = context.getConfiguration().getInt(
				Common.CONF_LOCATION_FILTER,  Common.DEFAULT_LOCATION_FILTER);
		DWELL_PERCENT_FILTER = context.getConfiguration().getDouble(
				Common.CONF_FILTER_DWELL_PERCENT, Common.DEFAULT_DWELL_PERCENT);
	};
	

	List<Customer> customerList = new ArrayList<Customer>();
	@Override
	public void reduce(Text key, Iterable<BytesWritable> values, Context context)
			throws IOException, InterruptedException {
		Customer.Builder total = Customer.newBuilder();
		Customer.Builder part = Customer.newBuilder();
		boolean hasTotal = false;
		boolean hasPart = false;

		for (BytesWritable val : values) {
			byte[] arr = val.getBytes();
			if (arr[0] == Common.MERGE_TAG_T) {
				total.clear().mergeFrom(val.getBytes(), 1, val.getLength() - 1);
				hasTotal = true;
				context.getCounter(JobCounter.TOTAL_REDUCE_OK).increment(1);
			} else if (arr[0] == Common.MERGE_TAG_P) {
				part.clear().mergeFrom(val.getBytes(), 1, val.getLength() - 1);
				hasPart = true;
				context.getCounter(JobCounter.PART_REDUCE_OK).increment(1);
			} else {
				context.getCounter(JobCounter.INVALID_RECORD).increment(1);
			}
		}
		
		if (!hasTotal) {
			context.getCounter(JobCounter.TOATL_IS_EMPTY);
			total.clear().mergeFrom(part.build());
		}
		else {
			if (!hasPart) {
				context.getCounter(JobCounter.PART_IS_EMPTY);
			}
			else {
				//build map with part
				Map<Coordinates, Location> locationMap = new HashMap<Coordinates, Location>();
				Set<Coordinates> coInTotal = new HashSet<Coordinates>();
				for (Location loc : part.getLocationList()) {
					Coordinates coordinates = new Coordinates(
							loc.getLocationX(), loc.getLocationY(), loc.getPlanarGraph());
					if (!locationMap.containsKey(coordinates)) {
						locationMap.put(coordinates, loc);
					}
					else {
						context.getCounter(JobCounter.PART_COORDINATE_CONFLIT).increment(1);
					}	
				}
				
				//update old locations
				for (Location.Builder loc : total.getLocationBuilderList()) {
					Coordinates coordinates = new Coordinates(
							loc.getLocationX(), loc.getLocationY(), loc.getPlanarGraph());
					if (locationMap.containsKey(coordinates)) {
						Location newLoc = locationMap.get(coordinates);
						loc.addAllTimeStamp(newLoc.getTimeStampList());
						//loc.addAllClientTime(newLoc.getClientTimeList());	
						coInTotal.add(coordinates);
						context.getCounter(JobCounter.PART_IN_OLD).increment(1);
					}
				}
				
				//add new locations to total
				Iterator<Coordinates> iter = locationMap.keySet().iterator();
				while (iter.hasNext()) {
					Coordinates co = iter.next();
					if (coInTotal.contains(co)) {
						continue;
					}
					total.addLocation(locationMap.get(co));
					context.getCounter(JobCounter.PART_NEW).increment(1);
				}
				
			}
		}
		int locCount = total.getLocationCount();
		if (locCount == 0) {
			context.getCounter(JobCounter.LOCATION_ZERO).increment(1);
			return;
		}
		
		for (Location.Builder lcb : total.getLocationBuilderList()) {
			 long planarGraph = lcb.getPlanarGraph();		 
			 if (!lcb.hasMallId() &&
					 FLOOR_MALL_MAP.containsKey(planarGraph)) {
				 lcb.setMallId(FLOOR_MALL_MAP.get(planarGraph));
			 }
		}

		int mallId = Common.getMallId(total.build());
		if (mallId == 0) {
			context.getCounter(JobCounter.MALL_ID_ERROR).increment(1);
			//return;
		}
		
		//TODO customer classify	
		Set<Long> floorSet = new HashSet<Long>();
		for (Location loc : total.getLocationList()) {
			floorSet.add(loc.getPlanarGraph());
		}
		
		long dwellInFrame = 0;
		long dwellOutFrame = 0;
		// if one of the locations is in frame, the customer is not passenger.
		for (Location loc : total.getLocationList()) {
			boolean locInFrame = false;
			List<Long> timeList = new ArrayList<Long>(loc.getTimeStampList());
			long dwell = TimeCluster.getTimeDwell(timeList, Common.DEFAULT_DWELL_PASSENGER);
			
			long planarGraph = loc.getPlanarGraph();
			Coordinates coordinate = new Coordinates(
					loc.getLocationX(), loc.getLocationY(), planarGraph);	
			if (FRAME_MAP.containsKey(planarGraph)) {
				//one floor may have two or three frames
				for (List<Coordinates> coorList : FRAME_MAP.get(planarGraph)) {
					Polygon polygon = new Polygon(coorList);
					if (polygon.coordinateInPolygon(coordinate)) {
						locInFrame = true;
						break;
					}
				}
			}
			
			if (locInFrame) {
				dwellInFrame += dwell;
			}
			else {
				dwellOutFrame += dwell;
			}
		}
	
		boolean inFrame = false;
		long dwellSum = dwellInFrame + dwellOutFrame;
		if (dwellSum != 0 
				&& (dwellInFrame * 1.0) / dwellSum > DWELL_PERCENT_FILTER ) {
			inFrame = true;
		}
		
		dwellSum = TimeCluster.getTimeDwell(total.build(), Common.DEFAULT_DWELL_PASSENGER)
				/ Common.MINUTE_FORMATER;
		
		if (inFrame) {
			String mac = new String(total.getPhoneMac().toByteArray());
			if (MACHINE_MAP.containsKey(mallId)
					&& MACHINE_MAP.get(mallId).contains(mac)) {
				total.setUserType(UserType.MACHINE);
			}
			else if (EMPLOYEE_MAP.containsKey(mallId)
					&& EMPLOYEE_MAP.get(mallId).contains(mac)) {
				total.setUserType(UserType.EMPLOYEE);
			}
			else {
				total.setUserType(UserType.CUSTOMER);
			}

//			if (dwellSum < PASSENGER_DWELL_FILTER * Common.MINUTE_FORMATER) {
			if (floorSet.size() < FLOOR_NUMBER_FILTER 
					&& locCount < LOCATION_NUMBER_FILTER
					&& dwellSum < PASSENGER_DWELL_FILTER) {
				total.setUserType(UserType.PASSENGER);
				context.getCounter(JobCounter.PASSENGER_NUM).increment(1);
			}

		}
		else {
			total.setUserType(UserType.PASSENGER);
			context.getCounter(JobCounter.PASSNEGER_OUTOF_FRAME).increment(1);
		}
			
		context.write(key, new BytesWritable(total.build().toByteArray()));	
		customerList.add(total.build());
	}
	
	@Override
	public void cleanup(Context context)
			throws IOException, InterruptedException {
//		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
//		long updateTime = 0;
//		try {
//			updateTime = timeFormat.parse(
//					context.getConfiguration().get(Common.MALL_SYSTEM_BIZDATE)).getTime();
//		} catch (ParseException e) {
//			e.printStackTrace();
//		}
//		
//		String mongoServerList = context.getConfiguration().get(
//				Common.MONGO_SERVER_LIST);
//		String serverArr[] = mongoServerList.split(Common.COMMA, -1);
//		if (serverArr.length != Common.MONGO_SERVER_NUM) {
//			throw new RuntimeException("Get mongo server fail.");
//		}
//		String mongoServerFst = serverArr[0];
//		String mongoServerSnd = serverArr[1];
//		String mongoServerTrd = serverArr[2];
//
//		String mongoDbName = context.getConfiguration().get(
//				Common.MONGO_DB_NAME);
//		int mongoServerPort = Integer.parseInt(context.getConfiguration().get(
//				Common.MONGO_SERVER_PORT));
//
//		MongoClient mongoClient = new MongoClient(Arrays.asList(
//				new ServerAddress(mongoServerFst, mongoServerPort),
//				new ServerAddress(mongoServerSnd, mongoServerPort),
//				new ServerAddress(mongoServerTrd, mongoServerPort)));
//		DB mongoDb = mongoClient.getDB(mongoDbName);
//		
//		String custCollectionName = context.getConfiguration().get(
//				Common.MONGO_COLLECTION_CUSTOMER);
//		DBCollection custCollection = 
//				mongoDb.getCollection(custCollectionName);
//		
//		String customerKey = context.getConfiguration().get(
//				Common.MONGO_COLLECTION_CUSTOMER_MAC);
//		String customerVal = context.getConfiguration().get(
//				Common.MONGO_COLLECTION_CUSTOMER_VALUE);
//		String updateTimeKey = context.getConfiguration().get(
//				Common.MONGO_COLLECTION_CUSTOMER_TIME);
//		
//		for (Customer cust : customerList) {
//			String mac = new String(cust.getPhoneMac().toByteArray());
//
//			BasicDBObject document = new BasicDBObject();
//			document.put(customerKey, mac);
//			document.put(customerVal, cust.toByteArray());
//			document.put(updateTimeKey, updateTime);
//			
//			BasicDBObject query = new BasicDBObject();
//			query.put(customerKey, mac);
//			
//			custCollection.update(query, document, true, false);
//		}
//		
//		mongoClient.close();

	}
	
	
}
