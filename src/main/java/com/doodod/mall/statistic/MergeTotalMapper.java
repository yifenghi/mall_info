/**
 * @author yupeng.cao@palmaplus.com
 */
package com.doodod.mall.statistic;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.mall.common.Common;

public class MergeTotalMapper  extends 
	Mapper<Text, BytesWritable, Text, BytesWritable> {
	enum JobCounter {
		TOATL_MAP_OK
	}
	
	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		byte[] val = value.getBytes();
		byte[] res = new byte[value.getLength() + 1];
		int index = 0;
		res[index++] = Common.MERGE_TAG_T;
		while (index < res.length) {
			res[index] = val[index - 1];
			index++;
		}

		context.getCounter(JobCounter.TOATL_MAP_OK).increment(1);
		BytesWritable out = new BytesWritable(res);
		context.write(key, out);
	}

}
