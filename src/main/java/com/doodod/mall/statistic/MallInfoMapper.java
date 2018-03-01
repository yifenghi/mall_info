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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import com.doodod.mall.common.Common;
import com.doodod.mall.common.Coordinates;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;
import com.google.protobuf.ByteString;

public class MallInfoMapper extends TableMapper <Text, BytesWritable> {	
	enum JobCounter {
		X_NUM,
		Y_NUM,
		Z_NUM,
		RECORD_NUM,
		QUALIFIER_NUM_ERROR,
		MAC_KEY_ERROR,
		MAC_NOT_IN_BRAND_MAP,
	}
	private static Map<String, String> MAC_BRAND_MAP = new HashMap<String, String>();
	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		Charset charSet = Charset.forName("UTF-8");
		String macBrandPath = context.getConfiguration().get(Common.CONF_MAC_BRAND);
		BufferedReader brandReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(macBrandPath), charSet));
		String line = "";
		while ((line = brandReader.readLine()) != null) {
			String arr[] = line.split(Common.CTRL_A, -1);
			if (arr.length < 2) {
				continue;
			}
			MAC_BRAND_MAP.put(arr[0], arr[1]);
		}
		brandReader.close();
	}
	@Override
	public void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		byte[] macArr = key.get();
		String macType = getMacFromArr(macArr, 0, Common.AP_MAC_LENGTH);
		String phoneMac = getMacFromArr(macArr, Common.AP_MAC_LENGTH, macArr.length);
		
		Customer.Builder cb = Customer.newBuilder();
		
		if (macType.equals(Common.TAG_MAC)) {
			cb.setPhoneMac(ByteString.copyFrom(phoneMac.getBytes()));
		}
		else {
			context.getCounter(JobCounter.MAC_KEY_ERROR).increment(1);
			return;
		}
		
		String phoneMacKey = phoneMac.substring(0, Common.MAC_KEY_LENGTH);
		String phoneBrand = Common.BRAND_UNKNOWN;
		if (MAC_BRAND_MAP.containsKey(phoneMacKey)) {
			phoneBrand = MAC_BRAND_MAP.get(phoneMacKey);
		}
		else {
			context.getCounter(JobCounter.MAC_NOT_IN_BRAND_MAP).increment(1);
		}
		cb.setPhoneBrand(ByteString.copyFrom(phoneBrand.getBytes()));
		
		List<Location.Builder> locationList = new ArrayList<Location.Builder>();
		List<Long> timeStampList = new ArrayList<Long>();
		List<Long> clientTimeList = new ArrayList<Long>();
		List<Long> planarGraphList = new ArrayList<Long>();
		List<Double> locationXList = new ArrayList<Double>();
		List<Double> locationYList = new ArrayList<Double>();
		List<String> positionSysList = new ArrayList<String>();

		for (Cell cell : value.listCells()) {
			long timeStamp = cell.getTimestamp();
			if (!timeStampList.contains(timeStamp)) {
				timeStampList.add(timeStamp);
			}
			
			String qualifier = new String(CellUtil.cloneQualifier(cell));
			//String qualifier = new String(
			//		cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
			
			if (Common.LOCATION_X.equals(qualifier)) {				
				double locationX = Bytes.toDouble(CellUtil.cloneValue(cell));
				locationXList.add(locationX);
				context.getCounter(JobCounter.X_NUM).increment(1);
			}
			else if (Common.LOCATION_Y.equals(qualifier)) {
				double locationY = Bytes.toDouble(CellUtil.cloneValue(cell));
				locationYList.add(locationY);
				context.getCounter(JobCounter.Y_NUM).increment(1);
			}
			else if (Common.LOCATION_Z.equals(qualifier)) {
				//int planarGraph = Bytes.toInt(CellUtil.cloneValue(cell));
				long planarGraph = Bytes.toLong(CellUtil.cloneValue(cell));

				planarGraphList.add(planarGraph);
				context.getCounter(JobCounter.Z_NUM).increment(1);
			}
			else if (Common.LOCATION_TIME.equals(qualifier)) {
				long clientTime = Bytes.toLong(CellUtil.cloneValue(cell));
				clientTimeList.add(clientTime);
			}
			else if (Common.LOCATION_PRO.equals(qualifier)) {
				String positionSys = Bytes.toString(CellUtil.cloneValue(cell));
				positionSysList.add(positionSys);
			}
		}
		
		if (timeStampList.size() != planarGraphList.size() 
				|| timeStampList.size() != locationXList.size()
				|| timeStampList.size() != locationYList.size()) {
			context.getCounter(JobCounter.QUALIFIER_NUM_ERROR).increment(1);
			return;
		}
		
		Coordinates last = new Coordinates(0, 0, 0);
		for (int i = timeStampList.size() -1 ; i >= 0; i--) {
			Coordinates current = new Coordinates(locationXList.get(i), 
					locationYList.get(i), planarGraphList.get(i));
			
			if (!current.equals(last)) {
				Location.Builder lb = Location.newBuilder();
				lb.addTimeStamp(timeStampList.get(i));
				lb.setLocationX(locationXList.get(i));
				lb.setLocationY(locationYList.get(i));
				lb.setPlanarGraph(planarGraphList.get(i));
				//lb.addClientTime(clientTimeList.get(i));
				lb.setPositionSys(
						ByteString.copyFrom(positionSysList.get(i).getBytes()));
				locationList.add(lb);
			}
			else {
				Location.Builder lb = locationList.get(locationList.size() - 1);
				lb.addTimeStamp(timeStampList.get(i));
				//lb.addClientTime(clientTimeList.get(i));
			}	
			
			last.set(current);		
		}
		
		for (Location.Builder lb : locationList) {
			cb.addLocation(lb);
		}
		
		context.getCounter(JobCounter.RECORD_NUM).increment(1);	
		context.write(new Text(phoneMac), new  BytesWritable(cb.build().toByteArray()));
	}
	
	private String getMacFromArr(byte[] arr, int start, int end) {
		StringBuffer mac = new StringBuffer();
		for (int i = start; i < end; i++) {
			mac.append(String.format(Common.MAC_FORMAT, arr[i]));
			mac.append(':');
		}
		mac.deleteCharAt(mac.length() - 1);
		return mac.toString();
	}
	
}
