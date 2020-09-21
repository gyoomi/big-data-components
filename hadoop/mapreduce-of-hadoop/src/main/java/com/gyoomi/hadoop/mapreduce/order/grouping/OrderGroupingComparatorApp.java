/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.mapreduce.order.grouping;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 求每个订单中金额最大的交易明细
 *
 * @author Leon
 * @date 2020-09-21 11:24
 */
public class OrderGroupingComparatorApp
{

	static class OrderGroupingComparatorMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>
	{
		OrderBean ob = new OrderBean();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String lineText = value.toString();
			String[] fields = StringUtils.split(lineText, "\t");
			ob.setItemId(new Text(fields[0]));
			ob.setAmount(new DoubleWritable(Double.parseDouble(fields[2])));

			context.write(ob, NullWritable.get());
		}
	}

	static class OrderGroupingComparatorReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable>
	{
		@Override
		protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
		{
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception
	{
		if (args.length <= 0)
		{
			args = new String[]{"D:/bigdata/order/order.txt", "D:/bigdata/orderoutput"};
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(OrderGroupingComparatorApp.class);

		job.setMapperClass(OrderGroupingComparatorMapper.class);
		job.setReducerClass(OrderGroupingComparatorReducer.class);

		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//指定shuffle所使用的GroupingComparator类
		// TODO
		job.setGroupingComparatorClass(ItemIdGroupingComparator.class);
		//指定shuffle所使用的partitioner类
		job.setPartitionerClass(ItemIdPartitioner.class);

		job.setNumReduceTasks(3);

		job.waitForCompletion(true);
		System.out.println("over");
	}

}
