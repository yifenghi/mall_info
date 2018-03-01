package com.doodod.mall.analyse;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;

public class TestMachineMapper extends 
	Mapper<Text, Text, Text, Text> {
	@Override
	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
		Text outKey = new Text(Common.DEFAULT_MALL_ID
				+ Common.CTRL_A + key.toString());
		context.write(outKey, value);
	}
}
