/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.mapreduce.mapsidejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 解决数据倾斜的问题
 *
 * @author Leon
 * @date 2020-09-08 9:05
 */
public class MapSideJoin
{

	static class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable>
	{
		private Map<String, String> productCache = new HashMap<>();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String lineText = value.toString();
			String[] orderFields = lineText.split("\t");
			String productName = productCache.get(orderFields[1]);
			Text orderKey = new Text(lineText + "\t" + productName);
			context.write(orderKey, NullWritable.get());
		}

	}

}
