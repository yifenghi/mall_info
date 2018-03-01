/**
 * @author yupeng.cao@palmaplus.com
 */
package com.doodod.mall.data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

import com.doodod.mall.common.Common;
public class shopPolygon {

	private static Map<Integer, String> publicAreaName = new HashMap<Integer, String>();// public area category_id and name

	public static String sendGetData(String get_url) throws Exception {
		URL getUrl = null;
		BufferedReader reader = null;

		HttpURLConnection connection = null;
		try {
			// get_url = URLEncoder.encode(get_url, "utf-8");
			getUrl = new URL(get_url);
			connection = (HttpURLConnection) getUrl.openConnection();
			connection.setRequestProperty("Accept", "application/json");
			connection.connect();

			return readStream(connection);
		} catch (Exception e) {
			throw e;
		} finally {
			if (reader != null) {
				reader.close();
				reader = null;
			}

			if (connection != null) {
				connection.disconnect();
			}
		}
	}

	public static void sendPostData(String POST_URL) throws Exception {
		POST_URL = "http://apiv2.palmap.cn";
		String content = "/mall/1667?appkey=123&udid=456";
		HttpURLConnection connection = null;
		DataOutputStream out = null;
		BufferedReader reader = null;
		try {
			URL postUrl = new URL(POST_URL);
			connection = (HttpURLConnection) postUrl.openConnection();
			connection.setDoOutput(true);// Let the run-time system (RTS) know
											// that we want input
			connection.setDoInput(true);// we want to do output.
			connection.setRequestMethod("POST");
			connection.setUseCaches(false);// can't use catch in post
			connection.setInstanceFollowRedirects(true);
			connection.setRequestProperty("Content-Type",// Specify the header
															// content type.
					"application/x-www-form-urlencoded");
			connection.setRequestProperty("Accept", "application/json");
			connection.connect();
			out = new DataOutputStream(connection.getOutputStream()); // Send
																		// POST
																		// output.
			// content = URLEncoder.encode(content, "utf-8");
			out.writeBytes(content);
			out.flush();
			out.close();

			System.out.println(readStream(connection));
		} catch (Exception e) {
			throw e;
		} finally {
			if (out != null) {
				out.close();
				out = null;
			}
			if (reader != null) {
				reader.close();
				reader = null;
			}

			if (connection != null) {
				connection.disconnect();
			}
		}
	}

	private static String readStream(HttpURLConnection connection)
			throws IOException {
		byte[] byteOut = new byte[1024];
		ByteArrayOutputStream outSteam = new ByteArrayOutputStream();

		int len = -1;
		while ((len = connection.getInputStream().read(byteOut)) != -1) {
			outSteam.write(byteOut, 0, len);
		}
		outSteam.close();

		return new String(outSteam.toByteArray());
	}

	private static void getShopPolygon(String type, String path, int floorId,
			String res) throws Exception {
		BufferedWriter writer = new BufferedWriter(new FileWriter(path, true));

		JSONObject floorInfo = new JSONObject(res);
		JSONArray layersArray = floorInfo.getJSONArray("layers");

		for (int i = 0; i < layersArray.length(); i++) {
			JSONObject obj = layersArray.getJSONObject(i);

			String layerType = obj.getString("layer_type");
			if (!layerType.equals(type)) {
				continue;
			}

			JSONObject featureObj = obj.getJSONObject("feature_collection");
			// featureObj.get("type") : FeatureCollection

			JSONArray featureArr = (JSONArray) featureObj.get("features");
			for (int j = 0; j < featureArr.length(); j++) {
				StringBuffer sb = new StringBuffer();
				sb.append(floorId);
				sb.append(Common.CTRL_A);

				JSONObject feature = featureArr.getJSONObject(j);

				JSONObject geometry = feature.getJSONObject("geometry");

				String geometryType = geometry.getString("type");
				StringBuffer polygonBuffer = new StringBuffer();
				if (geometryType.equals("Polygon")) {
					JSONArray coorArr = geometry.getJSONArray("coordinates");
					for (int a = 0; a < coorArr.length(); a++) {
						JSONArray coor = coorArr.getJSONArray(a);
						for (int b = 0; b < coor.length(); b++) {
							polygonBuffer.append(coor.getJSONArray(b)
									.getString(0));
							polygonBuffer.append(Common.CTRL_C);
							polygonBuffer.append(coor.getJSONArray(b)
									.getString(1));
							polygonBuffer.append(Common.CTRL_B);
						}
					}
					polygonBuffer.deleteCharAt(polygonBuffer.length() - 1);
				} else if (geometryType.equals("Point")) {
					JSONArray coorArr = geometry.getJSONArray("coordinates");
					polygonBuffer.append(coorArr.getString(0));
					polygonBuffer.append(Common.CTRL_C);
					polygonBuffer.append(coorArr.getString(1));
				}

				JSONObject properties = feature.getJSONObject("properties");

				sb.append(properties.get("id")).append(Common.CTRL_A);
				if (layerType.equalsIgnoreCase("shop")) {
					
					
					if("null".equalsIgnoreCase(""+properties.get("show_name"))){
						
						if (!publicAreaName.containsKey(properties
								.get("category_id"))) {
							sb.append("null").append(Common.CTRL_A);
						} else {
							sb.append(
									publicAreaName.get(properties
											.get("category_id"))).append(
									Common.CTRL_A);
						}
						
						
					}else{
						sb.append(properties.get("show_name"))
						.append(Common.CTRL_A);
					}
						
						
						
						
						
						
				} else if (layerType.equalsIgnoreCase("publicServicePoint")) {
					
					try {
						sb.append(properties.get("show_name")).append(
								Common.CTRL_A);
					} catch (Exception e) {
						if (!publicAreaName.containsKey(properties
								.get("category_id"))) {
							sb.append("null").append(Common.CTRL_A);
						} else {
							sb.append(
									publicAreaName.get(properties
											.get("category_id"))).append(
									Common.CTRL_A);
						}
						
					}

				}

				sb.append(properties.get("category_id")).append(Common.CTRL_A);
				sb.append(polygonBuffer);

				// System.out.print(sb.toString());
				writer.write(sb.toString());
				writer.newLine();
			}
			writer.close();
		}
	}

	public static List<Integer> getFloorIdList(int mallId, String hostName)
			throws Exception {
		List<Integer> floorList = new ArrayList<Integer>();

		// http://apiv2.palmap.cn/mall/67/floors?appkey=123&udid=456
		String res = sendGetData(hostName + "/mall/" + String.valueOf(mallId)
				+ "/floors?appkey=123&udid=456");

		JSONArray floorArray = new JSONArray(res);
		// index 0 is 平面图, skip
		for (int i = 1; i < floorArray.length(); i++) {
			JSONObject floorInfo = floorArray.getJSONObject(i);
			floorList.add(Integer.parseInt(floorInfo.get("id").toString()));
		}

		return floorList;
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("Usage: mallId output_dir");
		}
		// int mallId = Integer.parseInt(args[0]);
		// String path = args[1];
		// String hostName = args[2];
		int mallId = 67;// Common.DEFAULT_MALL_ID;
		String path = "F:\\data";
		String hostName = "http://apiv2.palmap.cn";
		// int mallId = Common.DEFAULT_MALL_ID;
		// String path =
		// "/Users/paul/Documents/code_dir/doodod/mall_info/data/";
		// String hostName = "http://apiv2.palmap.cn"

		List<Integer> floorIdList = getFloorIdList(mallId, hostName);

		String publicType = "publicServicePoint";
		String shopType = "shop";
		String frameType = "frame";

		String publicPath = path + "/" + publicType + ".new";
		String shopPath = path + "/" + shopType + ".new";
		String framePath = path + "/" + frameType + ".new";
		
		String publicAreaNamePath="E:\\workspace\\mall_info\\data\\publicArea_list";

		readPublicName(publicAreaNamePath);//read public area name

		for (Integer floorId : floorIdList) {
			String res = sendGetData(hostName + "/floor/"
					+ String.valueOf(floorId) + "?appkey=123&udid=456");
			getShopPolygon(publicType, publicPath, floorId, res);
			getShopPolygon(shopType, shopPath, floorId, res);
			getShopPolygon(frameType, framePath, floorId, res);
		}

		// getShopPolygon(type, path, floorId);
		// System.out.print(sendGetData("http://apiv2.palmap.cn/mall/67?appkey=123&udid=456"));
		// getFloorIdList(67);
	}

	public static void readPublicName(String path) throws IOException {
		Charset charSet = Charset.forName("UTF-8");

		String publicPath = path;
		BufferedReader publicReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(publicPath), charSet));
		String line = "";
		while ((line = publicReader.readLine()) != null) {
			String[] arrFrame = line.split(Common.CTRL_A, -1);
			if (arrFrame.length != 3) {
				
				continue;
			}

			publicAreaName.put(Integer.parseInt(arrFrame[0]),arrFrame[1]);
		}
		publicReader.close();
	}

}
