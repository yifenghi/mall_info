package com.doodod.mall.common;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class TestPlanarGraph extends TableMapper<Text, Text>{
	private static final String PLANAR_GRAPH = "PlanarGraph";
	
	@Override
	public void map(
			ImmutableBytesWritable key,
			Result value,
			Context context)
			throws IOException, InterruptedException {
		
		for (Cell cell : value.listCells()) {
			//long timeStamp = cell.getTimestamp();
			String qualifier = new String(CellUtil.cloneQualifier(cell));

			if (qualifier.equals(PLANAR_GRAPH)) {
				long planarGraph = Bytes.toLong(CellUtil.cloneValue(cell));
				//int planarGraph = Bytes.toShort(CellUtil.cloneValue(cell));
				//int planarGraph = Bytes.toInt(CellUtil.cloneValue(cell));

				context.write(new Text(PLANAR_GRAPH), new Text(String.valueOf(planarGraph)));
			}
		}
	}

}
