package com.doodod.mall.visit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class VisitCustomerLauncher extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new VisitCustomerLauncher(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		GenericOptionsParser options = new GenericOptionsParser(getConf(), args);
		Configuration hadoopConf = options.getConfiguration();
		
		Job job = new Job(hadoopConf);
		job.setJarByClass(VisitCustomerLauncher.class);
		job.setMapperClass(VisitCustomerMapper.class);
		job.setReducerClass(VisitCustomerReducer.class);
		//job.setCombinerClass(cls);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.waitForCompletion(true);
		
		return 0;
	}

}
