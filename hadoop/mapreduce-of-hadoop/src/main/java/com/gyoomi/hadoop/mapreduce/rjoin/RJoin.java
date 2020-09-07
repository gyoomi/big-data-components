/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.mapreduce.rjoin;


import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Mapreduce中的join操作
 *
 * @author Leon
 * @date 2020-09-04 10:06
 */
public class RJoin
{

	static class RjoinMapper extends Mapper<LongWritable, Text, Text, InfoBean>
	{

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			InfoBean infoBean = new InfoBean();

			String lineText = value.toString();
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			String name = inputSplit.getPath().getName();
			String productId = "";
			if (name.startsWith("order"))
			{
				String[] fields = lineText.split(",");
				productId = fields[2];
				infoBean.set(Integer.parseInt(fields[0]), fields[1], productId, Integer.parseInt(fields[3]), "", "0", 0, "0");
			}
			else
			{
				String[] fields = lineText.split(",");
				productId = fields[0];
				infoBean.set(0, "", productId, 0, fields[1], fields[2], Float.parseFloat(fields[3]), "1");
			}

			context.write(new Text(productId), infoBean);
		}

	}

	static class RJoinReducer extends Reducer<Text, InfoBean, InfoBean, NullWritable>
	{

		@Override
		protected void reduce(Text productId, Iterable<InfoBean> beans, Context context) throws IOException, InterruptedException
		{
			InfoBean pdBean = new InfoBean();
			List<InfoBean> orderBeans = new ArrayList<>();

			for (InfoBean bean : beans)
			{
				if ("1".equals(bean.getFlag()))
				{
					try
					{
						BeanUtils.copyProperties(pdBean, bean);
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
				}
				else
				{
					InfoBean oBean = new InfoBean();
					try
					{
						BeanUtils.copyProperties(oBean, bean);
						orderBeans.add(oBean);
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
				}
			}

			for (InfoBean bean : orderBeans)
			{
				bean.setProductName(pdBean.getProductName());
				bean.setCategoryId(pdBean.getCategoryId());
				bean.setPrice(pdBean.getPrice());

				context.write(bean, NullWritable.get());
			}
		}

	}

	public static void main(String[] args) throws Exception
	{
		if (null == args || args.length == 0)
		{
			throw new RuntimeException("args 参数为空");
		}
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(RJoin.class);

		job.setMapperClass(RjoinMapper.class);
		job.setReducerClass(RJoinReducer.class);

		job.setMapOutputValueClass(Text.class);
		job.setMapOutputValueClass(InfoBean.class);

		job.setOutputKeyClass(InfoBean.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 :1);
	}

}
