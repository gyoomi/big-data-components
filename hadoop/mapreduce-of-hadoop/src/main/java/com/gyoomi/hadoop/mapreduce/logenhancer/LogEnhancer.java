/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.mapreduce.logenhancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * LogEnhancer 入口类
 *
 * @author Leon
 * @date 2020-09-17 11:15
 */
public class LogEnhancer
{

	static class LogEnhancerMapper extends Mapper<LongWritable, Text, Text, NullWritable>
	{

		Map<String, String> ruleMap = new HashMap<>();

		Text k = new Text();

		NullWritable v = NullWritable.get();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			try
			{
				// 从数据库中加载规则信息倒ruleMap中
				DBLoader.dbLoader(ruleMap);
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			Counter counter = context.getCounter("malformed", "malformedline");
			String lineText = value.toString();
			String[] fields = lineText.split("\t");

			try
			{
				String url = fields[26];
				String contentTag = ruleMap.get(url);
				if (null == contentTag)
				{
					k.set(url + "\t" + "tocrawl" + "\n");
				}
				else
				{
					k.set(url + "\t" + contentTag + "\n");
				}

				context.write(k, v);
			}
			catch (Exception e)
			{
				counter.increment(1);
				// e.printStackTrace();
			}
		}

	}

	public static void main(String[] args) throws Exception
	{
		if (args.length <= 0)
		{
			args = new String[]{"D:/bigdata/logenhancer", "D:/bigdata/logenhanceroutput"};
		}
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(LogEnhancer.class);

		job.setMapperClass(LogEnhancerMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// 要控制不同的内容写往不同的目标路径，可以采用自定义outputformat的方法
		job.setOutputFormatClass(LogEnhancerOutputFormat.class);

		// TODO
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		// 尽管我们用的是自定义outputformat，但是它是继承制fileoutputformat
		// 在fileoutputformat中，必须输出一个_success文件，所以在此还需要设置输出path
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setNumReduceTasks(0);
		job.waitForCompletion(true);
		System.exit(0);
	}
}
