/**
 * @author yanshang.gong@palmaplus.com
 */
package com.doodod.mall.statistic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.doodod.mall.common.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.doodod.mall.message.Mall.Customer;
import com.doodod.mall.message.Mall.Location;
import com.doodod.mall.common.Common;

public class PassDoorReducer extends
        Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int inCount = 0;
        int outCount = 0;
        for (Text val : values) {
            String[] temp = val.toString().split(Common.CTRL_A, -1);
            if (temp[1].equals("0")) {
                inCount++;
            } else {
                outCount++;
            }
        }
        context.write(key, new Text(Integer.toString(inCount) + Common.CTRL_A + Integer.toString(outCount)));
    }

}
