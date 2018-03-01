/**
 * @author yupeng.cao@palmaplus.com
 */
package com.doodod.mall.common;

import java.util.HashSet;
import java.util.Set;

import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;

public class Common {
	public static final char MERGE_TAG_P = 'P';
	public static final char MERGE_TAG_T = 'T';

	
	public static final String CTRL_A  = "\u0001";
	public static final String CTRL_B  = "\u0002";
	public static final String CTRL_C  = "\u0003";
	public static final String COMMA   = ",";
		
	public static final String TABLE_NAME  = "Locations";
	public static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	public static final String DATE_FORMAT = "yyyy-MM-dd";
	public static final String MAC_FORMAT  = "%02x";
	public static final String NUM_FORNAT  = "0.00";
	
	public static final String MALL_SYSTEM_BIZDATE = "mall.system.bizdate";
	public static final String MALL_SYSTEM_TODAY   = "mall.system.today";
	public static final String MALL_SYSTEM_FLOORS  = "mall.system.floors";
	public static final String MALL_SYSTEM_COLUMNS = "mall.system.columns";
	

	public static final String BUSINESSTIME_START = "store.businesstime.start";
	public static final String BUSINESSTIME_NOW   = "store.businesstime.now";
	public static final String BUSINESSTIME_OUT   = "businesstime";
	
	public static final String MERGE_INPUT_PART  = "merge.input.part";
	public static final String MERGE_INPUT_TOTAL = "merge.input.total";
	
	public static final String LOCATION_X = "X";
	public static final String LOCATION_Y = "Y";
	public static final String LOCATION_Z = "PlanarGraph";
	public static final String LOCATION_PRO  = "PositioningSystem";
	public static final String LOCATION_TIME = "ClientTime";	
	public static final String FLOW_TIME_MIN  = "flow.time.min";
	public static final String FLOW_TIME_HOUR = "flow.time.hour";
	public static final String FLOW_TAG_MIN   = "min";
	public static final String FLOW_TAG_HOUR  = "hour";
	public static final String FLOW_TAG_DAY   = "day";
	public static final String FLOW_INPUT_MIN  = "flow.input.min";
	public static final String FLOW_INPUT_HOUR = "flow.input.hour";
	public static final String FLOW_INPUT_DAY  = "flow.input.day";

//	public static final String DWELL_TAG_SHOP   = "shop";
//	public static final String DWELL_TAG_FLOOR  = "floor";
	
	public static final String SHOP_CONF_FRAME  = "shop.conf.frame";
	public static final String SHOP_CONF_PUBLIC = "shop.conf.public";
	public static final String SHOP_CONF_STORE  = "shop.conf.store";
	public static final String SHOP_CONF_PUBLIC_AREA = "shop.conf.publicArea";
	public static final String SHOP_CONF_DISTANCE = "shop.public.distance";
	public static final String CUSTOMER_CONF_VIP  = "customer.conf.vip";
	public static final String CONF_MACHINE_LIST  = "conf.machine.list";
	public static final String CONF_EMPLOYEE_LIST = "conf.employee.list";
	public static final String CONF_PASSENGER_FILTER = "conf.passenger.filter";
	public static final String CONF_FLOOR_FILTER     = "conf.floor.filter";
	public static final String CONF_LOCATION_FILTER  = "conf.location.filter";
	public static final String CONF_FILTER_LIST      = "conf.filter.list";
	public static final String CONF_FLOORMALL_MAP    = "conf.floormall.map";
	public static final String CONF_FILTER_DWELL_PERCENT = "conf.filter.dwell.percent";

	public static final String CONF_MAC_BRAND     = "conf.mac.brand";
	public static final String CONF_BRAND_LIST    = "conf.brand.list";
	public static final String DEFAULT_MAC_BRAND  = "其他";
	public static final String BRAND_UNKNOWN      = "unknown";
	
	public static final String MONGO_OPTION_SET  = "$set";
	public static final String MONGO_SERVER_LIST = "mongo.server.list";
	public static final String MONGO_SERVER_PORT = "mongo.server.port";
	public static final String MONGO_DB_NAME     = "mongo.db.singlestore";
	
	public static final String MONGO_COLLECTION_MALL        = "mongo.collection.mall";
	public static final String MONGO_COLLECTION_MALL_ID     = "mongo.collection.mall.id";
	public static final String MONGO_COLLECTION_MALL_TAG    = "mongo.collection.mall.tag";
	public static final String MONGO_COLLECTION_MALL_TIME   = "mongo.collection.mall.time";
	public static final String MONGO_COLLECTION_MALL_NUMBER = "mongo.collection.mall.number";	
	
	public static final String MONGO_COLLECTION_FLOOR        = "mongo.collection.floor";
	public static final String MONGO_COLLECTION_FlOOR_ID     = "mongo.collection.floor.id";
	public static final String MONGO_COLLECTION_FlOOR_TAG    = "mongo.collection.floor.tag";
	public static final String MONGO_COLLECTION_FlOOR_TIME   = "mongo.collection.floor.time";
	public static final String MONGO_COLLECTION_FlOOR_NUMBER = "mongo.collection.floor.number";
	
	public static final String MONGO_COLLECTION_STORE        = "mongo.collection.shop";
	public static final String MONGO_COLLECTION_STORE_ID     = "mongo.collection.shop.id";
	public static final String MONGO_COLLECTION_STORE_CAT    = "mongo.collection.shop.cat";
	public static final String MONGO_COLLECTION_STORE_TAG    = "mongo.collection.shop.tag";
	public static final String MONGO_COLLECTION_STORE_TIME   = "mongo.collection.shop.time";
	public static final String MONGO_COLLECTION_STORE_NUMBER = "mongo.collection.shop.number";
	public static final String MONGO_COLLECTION_STORE_MALL   = "mongo.collection.shop.mall";

	public static final String MONGO_COLLECTION_CUSTOMER       = "mongo.collection.customer";
	public static final String MONGO_COLLECTION_CUSTOMER_MAC   = "mongo.collection.customer.mac";
	public static final String MONGO_COLLECTION_CUSTOMER_VALUE = "mongo.collection.customer.value";
	public static final String MONGO_COLLECTION_CUSTOMER_TIME  = "mongo.collection.customer.time";
	
	public static final String MONGO_COLLECTION_DWELL        = "mongo.collection.dwell";
	public static final String MONGO_COLLECTION_DWELL_USER   = "mongo.collection.dwell.user";
	public static final String MONGO_COLLECTION_DWELL_POS    = "mongo.collection.dwell.pos";
	public static final String MONGO_COLLECTION_DWELL_TYPE   = "mongo.collection.dwell.type";
	public static final String MONGO_COLLECTION_DWELL_TIME   = "mongo.collection.dwell.time";
	public static final String MONGO_COLLECTION_DWELL_NUMBER = "mongo.collection.dwell.number";
	
	public static final String MONGO_COLLECTION_VISIT        = "mongo.collection.visit";
	public static final String MONGO_COLLECTION_VISIT_ID     = "mongo.collection.visit.id";
	public static final String MONGO_COLLECTION_VISIT_TAG    = "mongo.collection.visit.tag";
	public static final String MONGO_COLLECTION_VISIT_TIME   = "mongo.collection.visit.time";
	public static final String MONGO_COLLECTION_VISIT_DWELL    = "mongo.collection.visit.dwell";
	public static final String MONGO_COLLECTION_VISIT_DWELLDIS = "mongo.collection.visit.dwelldis";
	public static final String MONGO_COLLECTION_VISIT_FLOOR    = "mongo.collection.visit.floor";
	public static final String MONGO_COLLECTION_VISIT_FLOORDIS = "mongo.collection.visit.floordis";
	public static final String MONGO_COLLECTION_VISIT_SHOP     = "mongo.collection.visit.shop";
	public static final String MONGO_COLLECTION_VISIT_SHOPDIS  = "mongo.collection.visit.shopdis";	
	public static final String MONGO_COLLECTION_VISIT_ZONE     = "mongo.collection.visit.zone";
	public static final String MONGO_COLLECTION_VISIT_ZONEDIS  = "mongo.collection.visit.zonedis";
	public static final String MONGO_COLLECTION_VISIT_TIMES    = "mongo.collection.visit.times";
	public static final String MONGO_COLLECTION_VISIT_TIMESDIS = "mongo.collection.visit.timesdis";
	public static final String MONGO_COLLECTION_VISIT_FREQ     = "mongo.collection.visit.freq";
	public static final String MONGO_COLLECTION_VISIT_FREQDIS  = "mongo.collection.visit.freqdis";
	public static final String MONGO_COLLECTION_VISIT_TAGTYPE  = "mongo.collection.visit.tagtype";
	public static final String MONGO_COLLECTION_VISIT_NEWCUST  = "mongo.collection.visit.newcust";
	public static final String MONGO_COLLECTION_VISIT_OLDCUST  = "mongo.collection.visit.oldcust";
	public static final String MONGO_COLLECTION_VISIT_SHOPMALL = "mongo.collection.visit.shopmall";

	public static final String MONGO_COLLECTION_LOCATION      = "mongo.collection.location";
	public static final String MONGO_COLLECTION_LOCATION_ID   = "mongo.collection.location.id";
	public static final String MONGO_COLLECTION_LOCATION_X    = "mongo.collection.location.x";
	public static final String MONGO_COLLECTION_LOCATION_Y    = "mongo.collection.location.y";
	public static final String MONGO_COLLECTION_LOCATION_NUM  = "mongo.collection.location.number";
	public static final String MONGO_COLLECTION_LOCATION_TIME = "mongo.collection.location.time";

	public static final String MONGO_COLLECTION_TRACE      = "mongo.collection.trace";
	public static final String MONGO_COLLECTION_TRACE_ID   = "mongo.collection.trace.id";
	public static final String MONGO_COLLECTION_TRACE_MALL = "mongo.collection.trace.mallid";
	public static final String MONGO_COLLECTION_TRACE_LIST = "mongo.collection.trace.list";
	public static final String MONGO_COLLECTION_TRACE_X    = "mongo.collection.trace.x";
	public static final String MONGO_COLLECTION_TRACE_Y    = "mongo.collection.trace.y";
	public static final String MONGO_COLLECTION_TRACE_Z    = "mongo.collection.trace.z";
	public static final String MONGO_COLLECTION_TRACE_TIME = "mongo.collection.trace.time";
	
	public static final String MONGO_COLLECTION_BRAND       = "mongo.collection.brand";
	public static final String MONGO_COLLECTION_BRAND_NAME  = "mongo.collection.brand.name";
	public static final String MONGO_COLLECTION_BRAND_COUNT = "mongo.collection.brand.count";
	public static final String MONGO_COLLECTION_BRAND_TIME  = "mongo.collection.brand.time";
	public static final String MONGO_COLLECTION_BRAND_MALL  = "mongo.collection.brand.mall";

	public static final String MONGO_COLLECTION_COUNT           = "mongo.collection.count";
	public static final String MONGO_COLLECTION_COUNT_ID        = "mongo.collection.count.id";
	public static final String MONGO_COLLECTION_COUNT_CUSTOMER  = "mongo.collection.count.customer";
	public static final String MONGO_COLLECTION_COUNT_EMPLOYEE  = "mongo.collection.count.employee";
	public static final String MONGO_COLLECTION_COUNT_MACHINE   = "mongo.collection.count.machine";
	public static final String MONGO_COLLECTION_COUNT_PASSENGER = "mongo.collection.count.passenger";
	public static final String MONGO_COLLECTION_COUNT_TOTAL     = "mongo.collection.count.total";
	public static final String MONGO_COLLECTION_COUNT_TIME      = "mongo.collection.count.time";
	
	public static final String LOCATION_DWELL_FILTER = "location.dwell.filter";
	public static final String DWELL_PER_LOCATION_FILTER = "dwell.per.location.filter";
	public static final String SHOP_DWELL_FILTER     = "shop.dwell.filter";

	public static final String VISIT_TIMES_FILTER  = "visit.times.filter";	
	public static final String VISIT_PERIOD_TOTAL  = "visit.period.total";
	public static final String VISIT_PERIOD_RECENT = "visit.period.recent";
	public static final String VISIT_PERIOD_FILTER = "visit.period.filter";

	
	public static final String TAG_MAC = "01";

   	public static final String NEARDOOR_LIMIT      = "neardoor.limit";
    public static final String NEARDOOR_TIME_START = "neardoor.time.start";
    public static final String NEARDOOR_TIME_END   = "neardoor.time.end";
    public static final String FLOORINFO_PATH      = "floorinfo.path";
    
    public static final String ANALYSE_CUSTOMER_MAC   = "customer.mac.path";
    public static final String ANALYSE_CUSTOMER_FLOOR = "customer.floor.id";
    
	public static final int AP_MAC_LENGTH    = 1;
	public static final int MONGO_SERVER_NUM = 3;
	public static final int MINUTE_FORMATER  = 60000;
	public static final int DAY_FORMATER  = 60000 * 60 * 24;

	public static final int DWELL_TYPE_SHOP  = 0;
	public static final int DWELL_TYPE_FLOOR = 1;
	public static final int DWELL_TYPE_MALL  = 2;
	public static final int DEFAULT_TAG_TYPE = 0;
	
	public static final int DEFAULT_MONGO_PORT = 27017;
	public static final int DEFAULT_COLUMN_NUM = 20;
	public static final int DEFAULT_FLOOR_NUM  = 6;
	public static final int DEFAULT_LOCATION_DWELL     = 10;
	public static final int DEFAULT_DWELL_PER_LOCATION = 25;
	public static final int DEFAULT_PASSENGER_FILTER   = 1;
	public static final int DEFAULT_FLOOR_FILTER       = 1;
	public static final int DEFAULT_LOCATION_FILTER    = 2;
	public static final int DEFAULT_VISIT_TIMES = 3;
	public static final int DEFAULT_MALL_ID     = 67;
	public static final int DEFAULT_MALL_SIZE   = 1;
	public static final int DEFAULT_PERIOD_TOTAL  = 15;
	public static final int DEFAULT_PERIOD_RECENT = 5;
	public static final int MAC_KEY_LENGTH        = 8;

	public static final double DEFAULT_DWELL_PERCENT = 0.8;
	public static final double DEFAULT_PERIOD_FLITER = 0.5;
	public static final int FILE_MALL_FLOOR_SIZE     = 2;
	public static final long DEFAULT_DWELL_PASSENGER = MINUTE_FORMATER * 15;
	public static final long DEFAULT_DWELL_SHOP_FILTER = MINUTE_FORMATER / 2;

	
	public static int getMallId(Customer customer) {
		int mallId = 0;
		
		Set<Integer> mallSet = new HashSet<Integer>();
		for (Location loc : customer.getLocationList()) {
			mallSet.add(loc.getMallId());
		}
		
		if (mallSet.size() == 1) {
			mallId = (Integer) mallSet.toArray()[0];
		}
		
		return mallId;
	}

}
