/**
 * @author yanshang.gong@palmaplus.com
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

public class PassDoorLauncher extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        GenericOptionsParser options = new GenericOptionsParser(getConf(), args);
        // String[] otherArgs = options.getRemainingArgs();
        Configuration hadoopConf = options.getConfiguration();

        Job job = new Job(hadoopConf);
        job.setJarByClass(PassDoorLauncher.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setMapperClass(PassDoorMapper.class);
        job.setReducerClass(PassDoorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
        return 0;
    }


    public static void main(String[] args) throws Exception {
        System.exit(
                ToolRunner.run(new Configuration(), new PassDoorLauncher(), args));
    }

}
