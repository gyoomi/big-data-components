/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.mapreduce.logwash;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 清洗 - main
 *
 * @author Leon
 * @date 2020-09-16 17:20
 */
public class LogWashPreProcessor
{
	static class LogWashPreProcessorMapper extends Mapper<LongWritable, Text, Text, NullWritable>
	{

		Text k = new Text();
		NullWritable v = NullWritable.get();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{

			String line = value.toString();
			WebLogBean webLogBean = WebLogParser.parse(line);
			//可以插入一个静态资源过滤（.....）
			/*WebLogParser.filterStaticResource(webLogBean);*/
			if (!webLogBean.isValid())
			{
				return;
			}
			k.set(webLogBean.toString());
			context.write(k, v);

		}

	}

	public static void main(String[] args) throws Exception
	{
		if (args.length <= 0)
		{
			args = new String[]{"D:/bigdata/logwash", "D:/bigdata/logwashoutput"};
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(LogWashPreProcessorMapper.class);

		job.setMapperClass(LogWashPreProcessorMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
