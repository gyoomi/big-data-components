/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.mapreduce.inverse;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 倒排索引 - 2
 *
 * @author Leon
 * @date 2020-09-10 16:25
 */
public class InverseIndexReduce
{

	static class InverseIndexReduceMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{

		}
	}


}
