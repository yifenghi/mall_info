/**
 * @author yanshang.gong@palmaplus.com
 */
package com.doodod.mall.statistic;

import java.awt.Point;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.print.attribute.Size2DSyntax;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;

import com.doodod.mall.data.*;
import com.doodod.mall.message.*;
import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;
//import com.google.protobuf.ByteString;

import org.apache.hadoop.util.MergeSort;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.awt.*;
import javax.swing.*;

public class NearDoor {

    public static double OFFSET_X = 1.2948790000E7;
    public static double OFFSET_Y = 4851350;
    //	public static double OFFSET_X = 0;
//	public static double OFFSET_Y = 0;
    public static int SCALE_X = 8;
    public static int SCALE_Y = 7;
    public static int IMAGE_LENGTH = 800;
    public static int IMAGE_HEIGHT = 770;
    public static double NEAR_LIMIT = 2; //½çÏÞÖ®ÄÚÈÏÎªÊÇÃÅµÄ¸½½ü£¬µ¥Î»ÊÇ×ø±ê£¬ÇøÓòÎªÔ²ÐÎ

    int minyDoor; //ÃÅµÄ×ø±ê±ß½ç£¬ÒÔ´Ë×÷ÎªÉÌ³¡µÄ±ß½ç
    int maxyDoor;
    int minxDoor;
    int maxxDoor;

    private List<Customer> customerList;
    private int floorID;
    private JSONObject floorInfo;  //Â¥²ãÐÅÏ¢£¬°üÀ¨ËùÓÐµêÆÌµÄ×ø±êµÈ£¬´ÓÍøÒ³ÖÐÅÀÏÂÀ´µÄ
    private DoorInfo doorInfo;  //ÃÅµÄÐÅÏ¢£¬´ÓÂ¥²ãÐÅÏ¢ÖÐÌôÑ¡³öÀ´
    private List<CustomersNearDoor> nearDoorCustomers = new ArrayList<CustomersNearDoor>(); //¿¿½ü¸÷¸öÃÅµÄÈËÔ±ÐÅÏ¢
    //private List<CustomerSorted> allSortedCustomer = new ArrayList<NearDoor.CustomerSorted>(); //ËùÓÐÈËÔ±µÄÅÅÐòÖ®ºóµÄÊ±¼ä´ÁºÍÎ»ÖÃ

    public NearDoor() {
    }

    public NearDoor(String filePath) throws IOException { // read the file of door info
        FileReader read = new FileReader(filePath);
        BufferedReader br = new BufferedReader(read);
        List<ShopCoor> doorCoors = new ArrayList<ShopCoor>();
        String row;
        while ((row = br.readLine()) != null) {
            String[] oneDoor = row.split("\t");
            double x = Double.parseDouble(oneDoor[2]);
            double y = Double.parseDouble(oneDoor[3]);
            ShopCoor sc = new ShopCoor(oneDoor[1], new LocPoint(x, y));
            doorCoors.add(sc);
        }
        br.close();
        read.close();
        doorInfo = new DoorInfo(doorCoors);
        ComputeMarketBound();
    }

    public NearDoor(JSONObject myFloorInfo) throws JSONException {
        floorID = Integer.parseInt(myFloorInfo.getString("id"));
        floorInfo = myFloorInfo;
        doorInfo = new DoorInfo(myFloorInfo);
        ComputeMarketBound();
    }

    public NearDoor(JSONObject myFloorInfo, List<Customer> customerList) throws JSONException {
        this.customerList = customerList;
        floorID = Integer.parseInt(myFloorInfo.getString("id"));
        floorInfo = myFloorInfo;
        doorInfo = new DoorInfo(myFloorInfo);
        for (ShopCoor sc : doorInfo.doorCoors) {
            nearDoorCustomers.add(getCustomersNearDoor(sc, customerList));
        }
        ComputeMarketBound();
    }

    private void ComputeMarketBound() {
        int[] xDoor = new int[doorInfo.getDoorNum()];
        int[] yDoor = new int[doorInfo.getDoorNum()];
        minyDoor = Integer.MAX_VALUE;
        maxyDoor = Integer.MIN_VALUE;
        minxDoor = Integer.MAX_VALUE;
        maxxDoor = Integer.MIN_VALUE;
        for (int i = 0; i < doorInfo.getDoorNum(); i++) {
            xDoor[i] = (int) doorInfo.getDoorCoor(i).getCoor(0).x;
            yDoor[i] = (int) doorInfo.getDoorCoor(i).getCoor(0).y;
            if (xDoor[i] < minxDoor) minxDoor = xDoor[i];
            if (xDoor[i] > maxxDoor) maxxDoor = xDoor[i];
            if (yDoor[i] < minyDoor) minyDoor = yDoor[i];
            if (yDoor[i] > maxyDoor) maxyDoor = yDoor[i];

        }
    }

    public JSONObject getFloorInfo() {
        return floorInfo;
    }

    public DoorInfo getDoorInfo() {
        return doorInfo;
    }

    public List<CustomersNearDoor> getNearDoorCustomersList() {
        return nearDoorCustomers;
    }

    public CustomersNearDoor getNearDoorCustomers(int idx) {
        return nearDoorCustomers.get(idx);
    }
//    public List<CustomerSorted> getAllCustomerSortedList(){
//    	return allSortedCustomer;
//    }

    //¸ù¾ÝÒ»¸öÃÅµÄ×ø±êºÍÄ³ÌìÈËÔ±É¨ÃèÐÅÏ¢£¬µÃµ½´ËÃÅ¸½½üµÄÈËÔ±ÐÅÏ¢
    private CustomersNearDoor getCustomersNearDoor(ShopCoor doorCoor, List<Customer> customerList) {
        LocPoint doorLoc = doorCoor.getCoorList().get(0);
        List<CustomerInfo> cusNearList = new ArrayList<CustomerInfo>();
        for (Customer cus : customerList) {
            for (Location cusLoc : cus.getLocationList()) {
                if ((Long) cusLoc.getPlanarGraph() == floorID && DistanceWithOffSet(doorLoc, cusLoc) < NEAR_LIMIT) {
                    cusNearList.add(new CustomerInfo(new String(cus.getPhoneMac().toByteArray()), new LocPoint(cusLoc.getLocationX(), cusLoc.getLocationY()), cusLoc.getTimeStampList()));
                }
            }
        }
        return (new CustomersNearDoor(doorCoor, cusNearList));
    }

    public CustomerSorted getCustomerSorted(String mac, List<Customer> customerList) {
        return new CustomerSorted(mac, customerList);
    }

    public CustomerSorted getCustomerSorted(Customer cus) {
        return new CustomerSorted(cus);
    }

    public void drawRoute() {
        new DrawRoute();
    }

    public void drawDoors() {
        new DrawDoor();
    }

    public void drawRoute(String mac) {
        new DrawRoute(mac);
    }

    public PassDoor getPassDoor(Customer cus) {  //¶ÔÒ»¸öÈËÔ±£¬¼ÆËã³öËùÓÐµÄ½ø³öÃÅµÄÊ±¼ä
        CustomerSorted cs = new CustomerSorted(cus);
        return getPassDoor(cs);
    }

    public PassDoor getPassDoor(CustomerSorted cs) {  //¶ÔÒ»¸öÈËÔ±£¬¼ÆËã³öËùÓÐµÄ½ø³öÃÅµÄÊ±¼ä
        List<Integer> doorValue = new ArrayList<Integer>();
        List<Integer> inOrOut = new ArrayList<Integer>();
        List<Long> timeStamp = new ArrayList<Long>();
        for (int i = 0; i < cs.getTimeAndLocSize() - 1; i++) {
            LocPoint lp1 = cs.getTimeAndLoc(i).getLocation();
            LocPoint lp2 = cs.getTimeAndLoc(i + 1).getLocation();
            if (i == 0 && isInMarket(lp1)) { //µÚÒ»¸öµãÔÚÉÌ³¡ÄÚ£¬ËµÃ÷ÔÚ×ß½øÀ´Ö®Ç°Ã»ÓÐÉ¨Ãèµ½
                int door;
                if ((door = getNearDoorValue(lp1)) == -1) //µÚÒ»¸öµã²»ÔÚÒ»¸öÃÅ¸½½ü£¬ËµÃ÷ÊÇ×ßµ½ÉÌ³¡ÖÐÐÄ²Å¿ªÊ¼É¨Ãèµ½£¬ÕâÑùÎÞ·¨¹À¼Æ³ö½øÃÅµÄÊ±¼ä
                    timeStamp.add((long) -1);
                else
                    timeStamp.add(cs.getTimeAndLoc(0).getTimeStamp()); //½øÃÅÊ±¼ä´óÖÂÎªµÚÒ»¸öÊ±¼ä´Á
                doorValue.add(door);
                inOrOut.add(0); //½ø
            } else if (isInMarket(lp1) != isInMarket(lp2)) {  //µ±Ç°Î»ÖÃºÍÏÂÒ»¸öÎ»ÖÃ·Ö±ð´¦ÓÚÉÌ³¡µÄÀïÃæºÍÍâÃæ£¬ÔòÈÏÎª¾­¹ýÃÅ

                double minDis = Double.MAX_VALUE;
                int minIdx = -1;
                if (lp2.x != lp1.x) {
                    //¼ÆËã³ölp1ºÍlp2Ëù¾ö¶¨µÄÖ±Ïßy=kx+bµÄ²ÎÊýkºÍb£¬¼´kx-y+b=0
                    double k = (lp2.y - lp1.y) / (lp2.x - lp1.x);
                    double b = lp1.y - k * lp1.x;

                    //¼ÆËã³ö¾àÀëÖ±Ïß×î½üµÄÃÅ
                    for (int j = 0; j < doorInfo.getDoorNum(); j++) {
                        LocPoint dp = doorInfo.getDoorCoor(j).getCoor(0);
                        double distance = Math.abs(k * dp.x - dp.y + b) / Math.sqrt(k * k + 1);
                        if (distance < minDis) {
                            minDis = distance;
                            minIdx = j;
                        }
                    }
                } else {
                    for (int j = 0; j < doorInfo.getDoorNum(); j++) {
                        LocPoint dp = doorInfo.getDoorCoor(j).getCoor(0);
                        //if (dp.y >= Math.min(lp1.y, lp2.y) && dp.y <= Math.max(lp1.y, lp2.y)) {
                            double distance = Math.abs(dp.x - lp1.x);
                            if (distance < minDis) {
                                minDis = distance;
                                minIdx = j;
                            }
                        //}
                    }
                }

                //·Ö±ð¼ÆËã³öÃÅµ½lp1ºÍlp2µÄ¾àÀë£¬¸ù¾Ý±ÈÀýËã³ö¾­¹ýÃÅÊ±µÄÊ±¼ä£¬¼ÙÉèÎªÔÈËÙ¡¢Ö±ÏßÐÐ×ß
                LocPoint dp = doorInfo.getDoorCoor(minIdx).getCoor(0);
                double dis1 = Distance(dp, lp1);
                double dis2 = Distance(dp, lp2);
                long time1 = cs.getTimeAndLoc(i).getTimeStamp();
                long time2 = cs.getTimeAndLoc(i + 1).getTimeStamp();
                long timePassDoor = (long) (time1 + (time2 - time1) * dis1 / (dis1 + dis2));

                doorValue.add(minIdx);
                inOrOut.add(isInMarket(lp1) ? 1 : 0);
                timeStamp.add(timePassDoor);
            }
        }
        LocPoint lp1 = cs.getTimeAndLoc(cs.getTimeAndLocSize() - 1).getLocation();
        if (isInMarket(lp1)) { //the last stamp
            int door;
            if ((door = getNearDoorValue(lp1)) == -1)
                timeStamp.add((long) -1);
            else
                timeStamp.add(cs.getTimeAndLoc(cs.getTimeAndLocSize() - 1).getTimeStamp());
            doorValue.add(door);
            inOrOut.add(1);
        }
        return (new PassDoor(cs.getMac(), doorValue, inOrOut, timeStamp));
    }

    public PassDoor getPassDoor(Customer cus, long startTime, long endTime) {  //¶ÔÒ»¸öÈËÔ±£¬¼ÆËã³öÔÚ¹æ¶¨Ê±¼ä¶ÎÄÚ½ø³öÃÅµÄÊ±¼ä
        long maxTime = Long.MIN_VALUE;
        for (int i = 0; i < cus.getLocationCount(); i++) {
            if (cus.getLocation(i).getTimeStamp(cus.getLocation(i).getTimeStampCount() - 1) > maxTime) {
                maxTime = cus.getLocation(i).getTimeStamp(cus.getLocation(i).getTimeStampCount() - 1);
            }
        }
        if (maxTime <= startTime) {
            return (new PassDoor());
        }
        CustomerSorted cs = new CustomerSorted(cus);
        return getPassDoor(cs, startTime, endTime);
    }

    public PassDoor getPassDoor(CustomerSorted cs, long startTime, long endTime) {  //¶ÔÒ»¸öÈËÔ±£¬¼ÆËã³öÔÚ¹æ¶¨Ê±¼ä¶ÎÄÚ½ø³öÃÅµÄÊ±¼ä
        List<Integer> doorValue = new ArrayList<Integer>();
        List<Integer> inOrOut = new ArrayList<Integer>();
        List<Long> timeStamp = new ArrayList<Long>();

        //µÃµ½ÔÚ¹æ¶¨Çø¼äÄÚµÄµÚÒ»¸öµãºÍ×îºóÒ»¸öµã
        int startIdx = -1, endIdx = -1;
        for (int i = 0; i < cs.getTimeAndLocSize(); i++) {
            if (startIdx == -1 && cs.getTimeStamp(i) > startTime) {
                startIdx = i;
            }
            if (cs.getTimeStamp(i) <= endTime) {
                endIdx = i;
            } else if (endIdx >= 0) break;
        }
        if (startIdx == -1 || endIdx == -1) {
            return (new PassDoor());
        }

        // µÚÒ»¸öµãÔÚÉÌ³¡ÄÚ²¿£¬Ç¿ÖÆ½ø
        LocPoint lp0 = cs.getTimeAndLoc(0).getLocation();
        if (startIdx == 0 && isInMarket(lp0)) {
            int door;
            if ((door = getNearDoorValue(lp0)) == -1) //µÚÒ»¸öµã²»ÔÚÒ»¸öÃÅ¸½½ü£¬ËµÃ÷ÊÇ×ßµ½ÉÌ³¡ÖÐÐÄ²Å¿ªÊ¼É¨Ãèµ½£¬ÕâÑùÎÞ·¨¹À¼Æ³ö½øÃÅµÄÊ±¼ä
                timeStamp.add((long) -1);
            else
                timeStamp.add(cs.getTimeAndLoc(0).getTimeStamp()); //½øÃÅÊ±¼ä´óÖÂÎªµÚÒ»¸öÊ±¼ä´Á
            doorValue.add(door);
            inOrOut.add(0); //½ø
        }

        //ºÍÖ¸¶¨Çø¼äÍâµÄÇ°Ò»¸öµãÁªÏµÆðÀ´
        if (startIdx > 0) {
            startIdx--;
        }

        //Ò»°ãÇé¿ö
        for (int i = startIdx; i <= endIdx - 1; i++) {
            LocPoint lp1 = cs.getTimeAndLoc(i).getLocation();
            LocPoint lp2 = cs.getTimeAndLoc(i + 1).getLocation();
            if (isInMarket(lp1) != isInMarket(lp2)) {  //µ±Ç°Î»ÖÃºÍÏÂÒ»¸öÎ»ÖÃ·Ö±ð´¦ÓÚÉÌ³¡µÄÀïÃæºÍÍâÃæ£¬ÔòÈÏÎª¾­¹ýÃÅ
                double minDis = Double.MAX_VALUE;
                int minIdx = -1;
                if (lp2.x != lp1.x) {
                    //¼ÆËã³ölp1ºÍlp2Ëù¾ö¶¨µÄÖ±Ïßy=kx+bµÄ²ÎÊýkºÍb£¬¼´kx-y+b=0
                    double k = (lp2.y - lp1.y) / (lp2.x - lp1.x);
                    double b = lp1.y - k * lp1.x;

                    //¼ÆËã³ö¾àÀëÖ±Ïß×î½üµÄÃÅ
                    for (int j = 0; j < doorInfo.getDoorNum(); j++) {
                        LocPoint dp = doorInfo.getDoorCoor(j).getCoor(0);
                        double distance = Math.abs(k * dp.x - dp.y + b) / Math.sqrt(k * k + 1);
                        if (distance < minDis) {
                            minDis = distance;
                            minIdx = j;
                        }
                    }
                } else {
                    for (int j = 0; j < doorInfo.getDoorNum(); j++) {
                        LocPoint dp = doorInfo.getDoorCoor(j).getCoor(0);
                       // if (dp.y >= Math.min(lp1.y, lp2.y) && dp.y <= Math.max(lp1.y, lp2.y)) {
                            double distance = Math.abs(dp.x - lp1.x);
                            if (distance < minDis) {
                                minDis = distance;
                                minIdx = j;
                            }
                        //}
                    }
                }

                //·Ö±ð¼ÆËã³öÃÅµ½lp1ºÍlp2µÄ¾àÀë£¬¸ù¾Ý±ÈÀýËã³ö¾­¹ýÃÅÊ±µÄÊ±¼ä£¬¼ÙÉèÎªÔÈËÙ¡¢Ö±ÏßÐÐ×ß
                LocPoint dp = doorInfo.getDoorCoor(minIdx).getCoor(0);
                double dis1 = Distance(dp, lp1);
                double dis2 = Distance(dp, lp2);
                long time1 = cs.getTimeAndLoc(i).getTimeStamp();
                long time2 = cs.getTimeAndLoc(i + 1).getTimeStamp();
                long timePassDoor = (long) (time1 + (time2 - time1) * dis1 / (dis1 + dis2));

                doorValue.add(minIdx);
                inOrOut.add(isInMarket(lp1) ? 1 : 0);
                timeStamp.add(timePassDoor);
            }
        }
        return (new PassDoor(cs.getMac(), doorValue, inOrOut, timeStamp));
    }

    public boolean isInMarket(LocPoint lp) { //ÅÐ¶ÏÒ»¸ö×ø±êÊÇ·ñÔÚÉÌ³¡ÀïÃæ£¬ÉÌ³¡Ä£ÐÍ¼ÙÉèÎª¾ØÐÎ
        if (lp.x > minxDoor && lp.x < maxxDoor && lp.y > minyDoor && lp.y < maxyDoor)
            return true;
        return false;
    }

    public int getNearDoorValue(LocPoint lp) {  //ÅÐ¶ÏÒ»¸ö×ø±êÊÇ·ñÔÚÒ»¸öÃÅ¸½½ü£¬ÊÇÔò·µ»ØÃÅµÄÖµ£¬²»ÊÇÔò·µ»Ø-1
        for (int i = 0; i < doorInfo.getDoorNum(); i++) {
            if (Distance(lp, doorInfo.getDoorCoor(i).getCoor(0)) < NEAR_LIMIT)
                return i;
        }
        return -1;
    }

    public class PassDoor {  //Ä³Ò»¸öÈË¶ÔÓÚËùÓÐÃÅµÄ½øÈëºÍ³öÈ¥µÄÊ±¼ä´Á¼ÇÂ¼
        private String mac;
        private List<Integer> doorValue; //ÃÅµÄÖµ£¬´Ó0¿ªÊ¼£¬ºÍdoorInfoÖÐµÄdoorÏÂ±êÏà¶ÔÓ¦
        private List<Integer> inOrOut; //0Îª½ø£¬1Îª³ö
        private List<Long> timeStamp; //½ø»ò³öÃÅµÄÊ±¼ä´Á
        private int passCount; //½ø³öÃÅµÄ×Ü´ÎÊý

        public PassDoor() {
            passCount = 0;
        }

        public PassDoor(String mac, List<Integer> doorValue, List<Integer> inOrOut, List<Long> timeStamp) {
            this.mac = mac;
            this.doorValue = doorValue;
            this.inOrOut = inOrOut;
            this.timeStamp = timeStamp;
            this.passCount = doorValue.size();
        }

        public String getMac() {
            return mac;
        }

        public List<Integer> getDoorValueList() {
            return doorValue;
        }

        public int getDoorValue(int idx) {
            return doorValue.get(idx);
        }

        public String getDoorName(int idx) {
            if (getDoorValue(idx) < 0) return "null";
            return doorInfo.getDoorCoor(getDoorValue(idx)).getShopName();
        }

        public List<Integer> getInOrOutList() {
            return inOrOut;
        }

        public int getInOrOut(int idx) {
            return inOrOut.get(idx);
        }

        public List<Long> getTimeStampList() {
            return timeStamp;
        }

        public long getTimeStamp(int idx) {
            return timeStamp.get(idx);
        }

        public int getPassCount() {
            return passCount;
        }
    }

    public class CustomersNearDoor { //Ä³Ò»¸öÃÅ¸½½üµÄÈËµÄÐÅÏ¢
        private ShopCoor doorCoor;
        private int customerNumber;
        private List<CustomerInfo> customerList;

        public CustomersNearDoor(ShopCoor doorcoor, List<CustomerInfo> list) {
            customerNumber = list.size();
            doorCoor = doorcoor;
            customerList = list;
        }

        public int getCustomerNumber() {
            return customerNumber;
        }

        public ShopCoor getDoorCoor() {
            return doorCoor;
        }

        public List<CustomerInfo> getCustomerList() {
            return customerList;
        }
    }

    public class CustomerInfo {  //ÔÚÃÅ¸½½ü±»É¨Ãèµ½µÄÒ»¸öÈËµÄÐÅÏ¢
        private String mac;
        private LocPoint cusCoor;
        private List<Long> timeStampList;

        public CustomerInfo(String macString, LocPoint cuscoor, List<Long> list) {
            mac = macString;
            cusCoor = cuscoor;
            timeStampList = list;
        }

        public String getMac() {
            return (mac);
        }

        public LocPoint getCusCoor() {
            return cusCoor;
        }

        public List<Long> getTimeStampList() {
            return timeStampList;
        }
    }

    public class CustomerSorted { //ÃÅ¸½½üÒ»¸öÈËµÄÐÅÏ¢£¬ËùÓÐÊ±¼ä´Á½øÐÐÅÅÐò
        private String mac;
        private List<TimeAndLoc> timeAndLocList = new ArrayList<TimeAndLoc>();
        private int timeAndLocSize;

        //        public CustomerSorted(String mac,List<TimeAndLoc> timeAndLocList){
//            this.mac = mac;
//            this.timeAndLocList = timeAndLocList;
//            this.timeAndLocSize = timeAndLocList.size();
//        }
        public String getMac() {
            return mac;
        }

        public int getTimeAndLocSize() {
            return timeAndLocSize;
        }

        public List<TimeAndLoc> getTimeAndLocList() {
            return timeAndLocList;
        }

        public TimeAndLoc getTimeAndLoc(int idx) {
            return timeAndLocList.get(idx);
        }

        public long getTimeStamp(int idx) {
            return timeAndLocList.get(idx).getTimeStamp();
        }

        public CustomerSorted(String mac, List<Customer> customerList) { //´ÓcustomerListÖÐÑ¡È¡Ö¸¶¨macµÄËùÓÐÊ±¼ä´Á²¢ÅÅÐò
            this.mac = mac;
            for (Customer cus : customerList) {
                String tempMac = new String(cus.getPhoneMac().toByteArray());
                if (tempMac.equals(mac)) {
                    List<Location> locationList = cus.getLocationList();
                    MergeSort(locationList);
                    break;
                }
            }
        }

        public CustomerSorted(Customer cus) { //´ÓcustomerListÖÐÑ¡È¡Ö¸¶¨macµÄËùÓÐÊ±¼ä´Á²¢ÅÅÐò
            this.mac = new String(cus.getPhoneMac().toByteArray());
            List<Location> locationList = cus.getLocationList();
            MergeSort(locationList);
        }

        public CustomerSorted(Customer cus, long startTime, long endTime) {
            this.mac = new String(cus.getPhoneMac().toByteArray());
            List<Location> locationList = cus.getLocationList();
            MergeSort(locationList, startTime, endTime);
        }

        private void MergeSort(List<Location> locationList, long startTime, long endTime) {
            timeAndLocList.clear();
            int ArrayNumber = locationList.size();
            int[] index = new int[ArrayNumber];  //Ã¿Ò»´Îµü´úÊ±£¬¸÷¸öÊý×éµ±Ç°Ö¸ÕëµÄÎ»ÖÃ
            while (true) {
                long min = Long.MAX_VALUE;
                int idx = -1;
                for (int i = 0; i < ArrayNumber; i++) {
                    if (index[i] == locationList.get(i).getTimeStampCount()) //ÒÑ¾­µ½ÁË×îÄ©Î²
                        continue;
                    long currentStamp = locationList.get(i).getTimeStamp(index[i]);
                    if (currentStamp < startTime || currentStamp > endTime) {
                        index[i]++;
                    } else {
                        if (currentStamp < min) {
                            min = locationList.get(i).getTimeStamp(index[i]);
                            idx = i;
                        }
                    }
                }
                if (idx == -1) break;
                index[idx]++;
                timeAndLocList.add(new TimeAndLoc(min, locationList.get(idx).getLocationX() - OFFSET_X, locationList.get(idx).getLocationY() - OFFSET_Y));
            }
            this.timeAndLocSize = timeAndLocList.size();
        }

        private void MergeSort(List<Location> locationList) {  //¹é²¢ÅÅÐò
            timeAndLocList.clear();
            int ArrayNumber = locationList.size();
            int[] index = new int[ArrayNumber];  //Ã¿Ò»´Îµü´úÊ±£¬¸÷¸öÊý×éµ±Ç°Ö¸ÕëµÄÎ»ÖÃ
            while (true) {
                long min = Long.MAX_VALUE;
                int idx = -1;
                for (int i = 0; i < ArrayNumber; i++) {
                    if (index[i] == locationList.get(i).getTimeStampCount()) //ÒÑ¾­µ½ÁË×îÄ©Î²
                        continue;
                    if (locationList.get(i).getTimeStamp(index[i]) < min) {
                        min = locationList.get(i).getTimeStamp(index[i]);
                        idx = i;
                    }
                }
                if (idx == -1) break;
                index[idx]++;
                timeAndLocList.add(new TimeAndLoc(min, locationList.get(idx).getLocationX() - OFFSET_X, locationList.get(idx).getLocationY() - OFFSET_Y));
            }
            this.timeAndLocSize = timeAndLocList.size();
        }
    }

    public class TimeAndLoc {  //´æ·ÅÒ»¸öÊ±¼ä´ÁºÍ´Ë¿ÌµÄ×ø±êÖµ
        private long timeStamp;
        private LocPoint loc;

        public TimeAndLoc(long time, LocPoint loc) {
            this.timeStamp = time;
            this.loc = loc;
        }

        public TimeAndLoc(long time, double x, double y) {
            this.timeStamp = time;
            this.loc = new LocPoint(x, y);
        }

        public long getTimeStamp() {
            return timeStamp;
        }

        public LocPoint getLocation() {
            return loc;
        }

        public double getLocX() {
            return loc.x;
        }

        public double getLocY() {
            return loc.y;
        }
    }

    public class DoorInfo { // ËùÓÐÃÅµÄÐÅÏ¢

        private List<ShopCoor> doorCoors;
        private int doorNum;

        public DoorInfo(JSONObject myFloorInfo) throws JSONException {
            floorInfo = myFloorInfo;
            doorCoors = getDoorCoors(myFloorInfo);
            doorNum = doorCoors.size();
        }

        public DoorInfo(List<ShopCoor> doorCoors) {
            this.doorCoors = doorCoors;
            doorNum = doorCoors.size();
        }

        public JSONObject getFloorInfo() {
            return floorInfo;
        }

        public List<ShopCoor> getDoorCoorList() {
            return doorCoors;
        }

        public ShopCoor getDoorCoor(int idx) {
            return doorCoors.get(idx);
        }

        public int getDoorNum() {
            return doorNum;
        }

        // µÃµ½Ä³Ò»²ãËùÓÐÃÅµÄ×ø±ê
        public List<ShopCoor> getDoorCoors(JSONObject myFloorInfo)
                throws JSONException {

            JSONArray layersArray = myFloorInfo.getJSONArray("layers");
            List<ShopCoor> res = new ArrayList<ShopCoor>();
            for (int i = 0; i < layersArray.length(); i++) {
                JSONObject obj = layersArray.getJSONObject(i);
                JSONArray jsonAryFeatures = obj.getJSONObject(
                        "feature_collection").getJSONArray("features");
                for (int j = 0; j < jsonAryFeatures.length(); j++) {
                    JSONObject jsonFeature = jsonAryFeatures.getJSONObject(j);
                    if (!jsonFeature.getJSONObject("properties").has("name"))
                        continue;
                    String name = jsonFeature.getJSONObject("properties")
                            .getString("name");
                    if (!name.contains("ÃÅ"))
                        continue;
                    String str = jsonFeature.getJSONObject("geometry")
                            .getString("coordinates");
                    JSONArray jsonAryCoor = (JSONArray) jsonFeature
                            .getJSONObject("geometry").getJSONArray(
                                    "coordinates");
                    List<LocPoint> coorList = new ArrayList<LocPoint>();
                    if (str.split("'").length > 2) // ²»Ö¹Ò»¸ö×ø±ê
                    {
                        jsonAryCoor = (JSONArray) jsonAryCoor.get(0);
                        for (int k = 0; k < jsonAryCoor.length(); j++) {
                            JSONArray jsonCoor = jsonAryCoor.getJSONArray(k);
                            coorList.add(new LocPoint(Double
                                    .parseDouble(jsonCoor.get(0).toString()) - OFFSET_X,
                                    Double.parseDouble(jsonCoor.get(1)
                                            .toString()) - OFFSET_Y));
                        }
                    } else {
                        coorList.add(new LocPoint((Double) jsonAryCoor.get(0) - OFFSET_X,
                                (Double) jsonAryCoor.get(1) - OFFSET_Y));
                    }
                    res.add(new ShopCoor(name, coorList));
                }
            }
            return res;
        }

    }

    public class ShopCoor { // ´æ·ÅµêÆÌµÄÃû³ÆºÍ×ø±ê
        private String shopName;
        private List<LocPoint> coorList = new ArrayList<LocPoint>();
        private int coorNumber;

        public ShopCoor() {
        }

        public ShopCoor(String name, List<LocPoint> list) {
            shopName = name;
            coorList = list;
            coorNumber = coorList.size();
        }

        public ShopCoor(String name, LocPoint lp) {
            shopName = name;
            coorList.add(lp);
            coorNumber = coorList.size();
        }

        public String getShopName() {
            return shopName;
        }

        public List<LocPoint> getCoorList() {
            return coorList;
        }

        public int getCoorNumber() {
            return coorNumber;
        }

        public LocPoint getCoor(int idx) {
            return coorList.get(idx);
        }
    }

    public class LocPoint { // ×ø±ê
        public double x;
        public double y;

        public LocPoint(double xx, double yy) {
            x = xx;
            y = yy;
        }

        public String ToString() {
            return ("[" + x + "," + y + "]");
        }

        public String ToStringWithSpace() {
            return (x + "\t" + y);
        }
    }

    public class DrawDoor extends JFrame {  // draw doors
        JLabel l1 = null;

        public DrawDoor() {
            setTitle("Â·ÏßÍ¼");
            l1 = new JLabel();
            l1.setText("Ò»²ãµÄÃÅ×ø±êºÍÉÌ³¡ÂÖÀªÍ¼");
            l1.setBounds(150, 100, 50, 200);
            this.add(l1);
            setLayout(new FlowLayout());
            setSize(IMAGE_LENGTH, IMAGE_HEIGHT);
            setPreferredSize(new Dimension(IMAGE_LENGTH, IMAGE_HEIGHT));
            setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            setVisible(true);
        }

        public void paint(Graphics g) {
            super.paint(g);

            LocPoint zx = new LocPoint(minxDoor * SCALE_X, IMAGE_HEIGHT - minyDoor * SCALE_Y);
            LocPoint zs = new LocPoint(minxDoor * SCALE_X, IMAGE_HEIGHT - maxyDoor * SCALE_Y);
            LocPoint yx = new LocPoint(maxxDoor * SCALE_X, IMAGE_HEIGHT - minyDoor * SCALE_Y);
            LocPoint ys = new LocPoint(maxxDoor * SCALE_X, IMAGE_HEIGHT - maxyDoor * SCALE_Y);

            g.drawLine((int) zx.x, (int) zx.y, (int) zs.x, (int) zs.y);
            g.drawLine((int) zs.x, (int) zs.y, (int) ys.x, (int) ys.y);
            g.drawLine((int) ys.x, (int) ys.y, (int) yx.x, (int) yx.y);
            g.drawLine((int) yx.x, (int) yx.y, (int) zx.x, (int) zx.y);

            for (ShopCoor sc : doorInfo.doorCoors) {
                g.fillOval((int) sc.getCoor(0).x * SCALE_X - 5, IMAGE_HEIGHT - (int) sc.getCoor(0).y * SCALE_Y - 5, 10, 10);
            }
        }
    }

    public class DrawRoute extends JFrame {
        JLabel l1 = null;
        String mac = "";

        public DrawRoute(String mac) {
            this.mac = mac;
            init();
        }

        public DrawRoute() {
            this.mac = "";
            init();
        }

        public void init() {
            setTitle("Â·ÏßÍ¼");
            l1 = new JLabel();
            l1.setText("Ò»²ãµÄÃÅ×ø±êºÍ¹Ë¿ÍÂ·ÏßÍ¼£¨ºÚÔ²µã£ºÃÅ£¬ºìÔ²È¦£ºÂ·ÏßÆðÊ¼µã£¬À¶Ô²È¦£ºÂ·Ïß½áÊøµã£©");
            l1.setBounds(150, 100, 50, 200);
            this.add(l1);
            setLayout(new FlowLayout());
            setSize(IMAGE_LENGTH, IMAGE_HEIGHT);
            setPreferredSize(new Dimension(IMAGE_LENGTH, IMAGE_HEIGHT));
            setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            setVisible(true);
        }

        public void paint(Graphics g) {
            super.paint(g);
            LocPoint zx = new LocPoint(minxDoor * SCALE_X, IMAGE_HEIGHT - minyDoor * SCALE_Y);
            LocPoint zs = new LocPoint(minxDoor * SCALE_X, IMAGE_HEIGHT - maxyDoor * SCALE_Y);
            LocPoint yx = new LocPoint(maxxDoor * SCALE_X, IMAGE_HEIGHT - minyDoor * SCALE_Y);
            LocPoint ys = new LocPoint(maxxDoor * SCALE_X, IMAGE_HEIGHT - maxyDoor * SCALE_Y);

            g.drawLine((int) zx.x, (int) zx.y, (int) zs.x, (int) zs.y);
            g.drawLine((int) zs.x, (int) zs.y, (int) ys.x, (int) ys.y);
            g.drawLine((int) ys.x, (int) ys.y, (int) yx.x, (int) yx.y);
            g.drawLine((int) yx.x, (int) yx.y, (int) zx.x, (int) zx.y);

            for (ShopCoor sc : doorInfo.doorCoors) {
                g.fillOval((int) sc.getCoor(0).x * SCALE_X - 5, IMAGE_HEIGHT - (int) sc.getCoor(0).y * SCALE_Y - 5, 10, 10);
            }
            Color[] colorArray = {Color.GREEN, Color.YELLOW, Color.GRAY, Color.DARK_GRAY, Color.ORANGE};
            int colorIdx = 0;
            if (mac == "") {  //»­³öÃÅ¸½½üµÄÈË
                for (int j = 0; j < getNearDoorCustomersList().size(); j++) {
                    CustomersNearDoor customers = getNearDoorCustomers(j);
                    for (CustomerInfo cus : customers.getCustomerList()) {
                        NearDoor.CustomerSorted cs = getCustomerSorted(cus.getMac(), customerList);
                        int[] xPoint = new int[cs.getTimeAndLocSize()];
                        int[] yPoint = new int[cs.getTimeAndLocSize()];
                        for (int i = 0; i < cs.getTimeAndLocSize(); i++) {
                            NearDoor.TimeAndLoc tl = cs.getTimeAndLoc(i);
                            xPoint[i] = (int) tl.getLocX() * NearDoor.SCALE_X;
                            yPoint[i] = IMAGE_HEIGHT - (int) tl.getLocY() * NearDoor.SCALE_Y;
                        }
                        g.setColor(Color.RED);
                        g.drawOval(xPoint[0], yPoint[0], 5, 5); //Æðµã
                        g.setColor(Color.BLUE);
                        g.drawOval(xPoint[cs.getTimeAndLocSize() - 1], yPoint[cs.getTimeAndLocSize() - 1], 5, 5); //ÖÕµã
                        g.setColor(colorArray[colorIdx]);
                        colorIdx = (colorIdx + 1) % colorArray.length;
                        g.drawPolyline(xPoint, yPoint, cs.getTimeAndLocSize()); //Â·Ïß
                    }
                }
            } else {
                for (Customer cus : customerList) {
                    NearDoor.CustomerSorted cs = getCustomerSorted(cus);
                    if (cs.getMac().equals(mac)) {
                        int[] xPoint = new int[cs.getTimeAndLocSize()];
                        int[] yPoint = new int[cs.getTimeAndLocSize()];
                        for (int i = 0; i < cs.getTimeAndLocSize(); i++) {
                            NearDoor.TimeAndLoc tl = cs.getTimeAndLoc(i);
                            xPoint[i] = (int) tl.getLocX() * NearDoor.SCALE_X;
                            yPoint[i] = IMAGE_HEIGHT - (int) tl.getLocY() * NearDoor.SCALE_Y;
                        }
                        g.setColor(Color.RED);
                        g.drawOval(xPoint[0], yPoint[0], 5, 5); //Æðµã
                        g.setColor(Color.BLUE);
                        g.drawOval(xPoint[cs.getTimeAndLocSize() - 1], yPoint[cs.getTimeAndLocSize() - 1], 5, 5); //ÖÕµã
                        g.setColor(colorArray[colorIdx]);
                        colorIdx = (colorIdx + 1) % colorArray.length;
                        g.drawPolyline(xPoint, yPoint, cs.getTimeAndLocSize()); //Â·Ïß
                        break;
                    }

                }
            }

        }
    }

    public double Distance(LocPoint a, LocPoint b) {
        return (Math.sqrt(Math.pow(a.x - b.x, 2) + Math.pow(a.y - b.y, 2)));
    }

    public double Distance(LocPoint a, Location b) {
        return (Math.sqrt(Math.pow(a.x - b.getLocationX(), 2) + Math.pow(a.y - b.getLocationY(), 2)));
    }

    public double DistanceWithOffSet(LocPoint a, Location b) {  // a is a result with offset,while b is original
        return (Math.sqrt(Math.pow(a.x + OFFSET_X - b.getLocationX(), 2) + Math.pow(a.y + OFFSET_Y - b.getLocationY(), 2)));
    }


}
