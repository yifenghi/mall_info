package com.doodod.mall.analyse;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.doodod.mall.common.Common;

public class CustomerAtFloorReducer extends 
	Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, 
			Context context) throws IOException, InterruptedException {
		String visit = "";
		String dwell = "";
		for (Text text : values) {
			String val = text.toString();
			if (val.startsWith("0")) {
				dwell = val;
			}
			else if (val.startsWith("1")) {
				visit = val;
			}
			else {
				context.getCounter("ANALYSE", "TAG_INVALID").increment(1);
			}
		}
		
		if (dwell.equals("")) {
			context.getCounter("ANALYSE", "HAS_NO_DWELL").increment(1);
			return;
		}
		
		visit = visit.substring(2);
		dwell = dwell.substring(2);
		
		Text out = new Text(visit + Common.CTRL_A + dwell);
		context.write(key, out);
	}

}
