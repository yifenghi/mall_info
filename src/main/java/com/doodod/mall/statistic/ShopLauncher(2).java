/**
 * @author yupeng.cao@palmaplus.com
 */
package com.doodod.mall.statistic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ShopLauncher extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		GenericOptionsParser options = new GenericOptionsParser(getConf(), args);
		// String[] otherArgs = options.getRemainingArgs();
		Configuration hadoopConf = options.getConfiguration();

		Job job = new Job(hadoopConf);
		job.setJarByClass(ShopLauncher.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(ShopMapper.class);
		//job.setReducerClass(ShopReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.waitForCompletion(true);
		return 0;
	}
	

	public static void main(String[] args) throws Exception {
		System.exit(
				ToolRunner.run(new Configuration(), new ShopLauncher(), args));
	}

}
