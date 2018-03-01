package com.doodod.mall.analyse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.doodod.mall.common.Common;

public class CustomerAtFloorLauncher extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		System.exit(
				ToolRunner.run(new Configuration(), new CustomerAtFloorLauncher(), args));
	}

	public int run(String[] args) throws Exception {
		GenericOptionsParser options = new GenericOptionsParser(getConf(), args);
		Configuration hadoopConf = options.getConfiguration();
		
		Job job = new Job(hadoopConf);
		job.setJarByClass(CustomerAtFloorLauncher.class);
		
		MultipleInputs.addInputPath(job, 
				new Path(hadoopConf.get(Common.MERGE_INPUT_PART)), 
				SequenceFileInputFormat.class, CustomerAtFloorMapper.class);
		MultipleInputs.addInputPath(
				job, new Path(hadoopConf.get(Common.MERGE_INPUT_TOTAL)), 
				SequenceFileInputFormat.class, CustomerVisitMapper.class);
		
		job.setReducerClass(CustomerAtFloorReducer.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
		return 0;
	}

}
