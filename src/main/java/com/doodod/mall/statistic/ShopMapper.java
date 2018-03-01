/**
 * @author yupeng.cao@palmaplus.com
 */
package com.doodod.mall.statistic;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;
import com.doodod.mall.common.Coordinates;
import com.doodod.mall.common.KDTree;
import com.doodod.mall.common.ListMedian;
import com.doodod.mall.common.Point;
import com.doodod.mall.common.Polygon;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;

public class ShopMapper extends 
	Mapper<Text, BytesWritable, Text, BytesWritable> {
	//LOC_OK = POINT_OUTOF_FRAME + PASSENGER_OK + PUBLIC_OK + POINT_IN_SHOP
	enum JobCounter {
		PUBLIC_FROMAT_ERROR,
		PUBLIC_COOR_ERROR,
		PUBLIC_COOR_CONFLICT,
		STORE_FROMAT_ERROR,
		STORE_COOR_ERROR,
		STORE_COOR_CONFLICT,
		POINT_OUTOF_FRAME,
		POINT_IN_SHOP,
		FLOORID_NOT_IN_MAP,
		PASSENGER_OK,
		MAP_OK,
		LOC_OK,
		PUBLIC_OK,
		SHOP_ID_CAT_NULL,
	}
	
	Map<Long, Map<Point, String>> publicMap = new HashMap<Long, Map<Point,String>>();
	Map<Long, Map<String, List<Coordinates>>> storeMap = new HashMap<Long, Map<String, List<Coordinates>>>();
	Map<Long, List<List<Coordinates>>> frameMap = new HashMap<Long, List<List<Coordinates>>>();
	Map<Long, Map<Point, List<String>>> coorStoreMap = new HashMap<Long, Map<Point, List<String>>>();
	Map<Long, KDTree> floorShopTreeMap = new HashMap<Long, KDTree>();
	Map<Long, KDTree> floorPublicTreeMap = new HashMap<Long, KDTree>();


	private static final String SHOP_DEFAULT = "0" + Common.CTRL_A + "0";
	private static int PUBLIC_DISTANCE = 1;
 	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		Charset charSet = Charset.forName("UTF-8");
		PUBLIC_DISTANCE = context.getConfiguration().getInt(Common.SHOP_CONF_DISTANCE, 1);

		String publicPath = context.getConfiguration().get(Common.SHOP_CONF_PUBLIC);
		BufferedReader publicReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(publicPath), charSet));
		String line = "";
		while ((line = publicReader.readLine()) != null) {
			String[] arrBrand = line.split(Common.CTRL_A, -1);
			if (arrBrand.length != 4) {
				context.getCounter(JobCounter.PUBLIC_FROMAT_ERROR).increment(1);		
				continue;
			}
			long floor = Long.parseLong(arrBrand[0]);
			String shopInfo = arrBrand[1] + Common.CTRL_A + arrBrand[2];
			
			String coorArr[] = arrBrand[3].split(Common.CTRL_C, -1);
			if (coorArr.length != 2) {
				context.getCounter(JobCounter.PUBLIC_COOR_ERROR).increment(1);
				continue;
			}
			double x = Double.parseDouble(coorArr[0]);
			double y = Double.parseDouble(coorArr[1]);
			
			//Coordinates coordinate = new Coordinates(x, y, floor);
			Point point = new Point(x, y);
			
			if (publicMap.containsKey(floor)) {
				Map<Point, String> item = publicMap.get(floor);
				if (item.containsKey(point)) {
					context.getCounter(JobCounter.PUBLIC_COOR_CONFLICT).increment(1);
				}
				else {
					item.put(point, shopInfo);
				}
			}
			else {
				Map<Point, String> item = new HashMap<Point, String>();
				item.put(point, shopInfo);
				publicMap.put(floor, item);
			}
			
		}
		publicReader.close();
		
		//build kd_tree with public coordinates in a floor
		Iterator<Long> floorIter = publicMap.keySet().iterator();
		while (floorIter.hasNext()) {
			long floorId = floorIter.next();
			Map<Point, String> coorMap = publicMap.get(floorId);
			List<Point> pointList = new ArrayList<Point>();
			Iterator<Point> publicIter = coorMap.keySet().iterator();
			while (publicIter.hasNext()) {
				pointList.add(publicIter.next());		
			}
			
			ListMedian median = new ListMedian();
			List<Point> medianList = median.getMidianList(pointList);
			
			KDTree tree = new KDTree();
			for (Point point : medianList) {
				tree.insert(point);
			}
			
			floorPublicTreeMap.put(floorId, tree);	
		}
		
		String storePath = context.getConfiguration().get(Common.SHOP_CONF_STORE);
		BufferedReader storeReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(storePath), charSet));
		line = "";
		while ((line = storeReader.readLine()) != null) {
			String[] arrBrand = line.split(Common.CTRL_A, -1);
			if (arrBrand.length != 4) {
				context.getCounter(JobCounter.STORE_FROMAT_ERROR).increment(1);		
				continue;
			}
			long floor = Long.parseLong(arrBrand[0]);
			String shopInfo = arrBrand[1] + Common.CTRL_A + arrBrand[2];
			
			String coorList[] = arrBrand[3].split(Common.CTRL_B, -1);
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
				
				Point point = coordinate.getPoint();
				if (coorStoreMap.containsKey(floor)) {
					Map<Point, List<String>> item = coorStoreMap.get(floor);
					if (item.containsKey(point)) {
						item.get(point).add(shopInfo);
					}
					else {
						List<String> arrList = new ArrayList<String>();
						arrList.add(shopInfo);
						item.put(point, arrList);
					}
				}
				else {
					Map<Point, List<String>> item = new HashMap<Point, List<String>>();
					List<String> arrList = new ArrayList<String>();
					arrList.add(shopInfo);
					item.put(point, arrList);
					coorStoreMap.put(floor, item);
				}

			}
			
			if (storeMap.containsKey(floor)) {
				storeMap.get(floor).put(shopInfo, coordinatesList);
			}
			else {
				Map<String, List<Coordinates>> item = new HashMap<String, List<Coordinates>>();
				item.put(shopInfo, coordinatesList);
				storeMap.put(floor, item);
			}
		}
		storeReader.close();
		
		//build kd_tree with shop coordinates in a floor
		//Iterator<Integer> floorIter = coorStoreMap.keySet().iterator();
		floorIter = coorStoreMap.keySet().iterator();
		while (floorIter.hasNext()) {
			long floorId = floorIter.next();
			Map<Point, List<String>> item = coorStoreMap.get(floorId);
			Iterator<Point> pointIterator = item.keySet().iterator();
			List<Point> pointList = new ArrayList<Point>();
			while (pointIterator.hasNext()) {
				pointList.add(pointIterator.next());
			}
			ListMedian median = new ListMedian();
			List<Point> medianList = median.getMidianList(pointList);
			
			KDTree tree = new KDTree();
			for (Point point : medianList) {
				tree.insert(point);
			}
			
			floorShopTreeMap.put(floorId, tree);			
		}
		
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
			//String shopInfo = arrFrame[0] + Common.CTRL_A + arrFrame[1];
			
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
			
			if (frameMap.containsKey(floor)) {
				frameMap.get(floor).add(coordinatesList);
			}
			else {
				List<List<Coordinates>> list = new ArrayList<List<Coordinates>>();
				list.add(coordinatesList);
				frameMap.put(floor, list);
			}
		}
		frameReader.close();
		
	}

	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		Customer.Builder cb = Customer.newBuilder();
		cb.mergeFrom(value.getBytes(), 0, value.getLength());
		context.getCounter(JobCounter.MAP_OK).increment(1);
		
		for(Location.Builder loc: cb.getLocationBuilderList()) {
			context.getCounter(JobCounter.LOC_OK).increment(1);
			long planarGraph = loc.getPlanarGraph();
			Coordinates coordinate = new Coordinates(
					loc.getLocationX(), loc.getLocationY(), planarGraph);
			loc.clearLocationX().clearLocationY();

			
			String shopInfo = SHOP_DEFAULT;
			
			boolean inPublic = false;
			boolean inFrame = false;

			if (publicMap.containsKey(planarGraph)) {
				KDTree tree = floorPublicTreeMap.get(planarGraph);
				Point point = tree.nearest(coordinate.getPoint());
				
				if( point.getDistance(coordinate.getPoint()) < PUBLIC_DISTANCE) {
					Map<Point, String> itemMap = publicMap.get(planarGraph);
					if (itemMap.containsKey(point)) {
						shopInfo = itemMap.get(point);
						inPublic = true;
						context.getCounter(JobCounter.PUBLIC_OK).increment(1);
					}
				} 
					
			}
			
			if (!inPublic) {
				if (frameMap.containsKey(planarGraph)) {
					for (List<Coordinates> list : frameMap.get(planarGraph)) {
						Polygon polygon = new Polygon(list);
						if (polygon.coordinateInPolygon(coordinate)) {
							inFrame = true;
							break;
						}
					}
					
					if (!inFrame) {
						context.getCounter(JobCounter.POINT_OUTOF_FRAME).increment(1);
						continue;
					}
					
					if (inFrame && storeMap.containsKey(planarGraph)) {
						KDTree tree = floorShopTreeMap.get(planarGraph);
						Point point = tree.nearest(coordinate.getPoint());
						List<String> storeList = coorStoreMap.get(planarGraph).get(point);
						
						Map<String, List<Coordinates>> itemMap = storeMap.get(planarGraph);

						for (String item : storeList) {
							List<Coordinates> list = itemMap.get(item);
							Polygon polygon = new Polygon(list);
							if (polygon.coordinateInPolygon(coordinate)) {
								shopInfo = item;
								context.getCounter(JobCounter.POINT_IN_SHOP).increment(1);
								break;
							}
						}
					}
				}
				else {
					context.getCounter(JobCounter.FLOORID_NOT_IN_MAP).increment(1);
				}
			}

			if (inFrame && shopInfo.equals(SHOP_DEFAULT)) {
				context.getCounter(JobCounter.PASSENGER_OK).increment(1);
				//continue;
			}
			
			String attr[] = shopInfo.split(Common.CTRL_A, -1);
			
			int shopId = 0;
			int shopCat = 0;
			try {
				shopId = Integer.parseInt(attr[0]);
				shopCat = Integer.parseInt(attr[1]);
			} catch (Exception e) {
				context.getCounter(JobCounter.SHOP_ID_CAT_NULL).increment(1);
			} 
			loc.setShopId(shopId).setShopCat(shopCat);
 		}
		
		context.write(key, new BytesWritable(cb.build().toByteArray()));
	}
}
