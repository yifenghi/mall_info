package com.doodod.mall.visit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.doodod.mall.common.Common;

public class VisitMallLauncher extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		System.exit(
				ToolRunner.run(new Configuration(), new VisitMallLauncher(), args));
	}

	public int run(String[] args) throws Exception {
		GenericOptionsParser options = new GenericOptionsParser(getConf(), args);
		Configuration hadoopConf = options.getConfiguration();
		
		Job job = new Job(hadoopConf);
		job.setJarByClass(VisitMallLauncher.class);
		job.setReducerClass(VisitMallReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		MultipleInputs.addInputPath(job,
				new Path(hadoopConf.get(Common.MERGE_INPUT_PART)),
				SequenceFileInputFormat.class, VisitMallMapper.class);
		MultipleInputs.addInputPath(job,
				new Path(hadoopConf.get(Common.MERGE_INPUT_TOTAL)),
				SequenceFileInputFormat.class, VisitMerger.class);
		job.waitForCompletion(true);
		return 0;
	}

}
