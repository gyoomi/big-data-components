package com.gyoomi.hadoop.mapreduce.province.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Flow count
 *
 * @author Leon
 * @version 2020/8/30 16:28
 */
public class FlowSum
{

	static class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean>
	{
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String lineText = value.toString();
			String[] words = lineText.split("\t");
			// 1. 取出手机号码
			String phoneNumber = words[1];
			// 2. 获取上行和下行流量
			long upFlow = Long.parseLong(words[words.length - 3]);
			long downFlow = Long.parseLong(words[words.length - 2]);

			context.write(new Text(phoneNumber), new FlowBean(upFlow, downFlow));
		}
	}

	static class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean>
	{
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException
		{
			long sumUpFlow = 0;
			long sumDownFlow = 0;
			for (FlowBean value : values)
			{
				sumUpFlow += value.getUpFlow();
				sumDownFlow = value.getDownFlow();
			}

			context.write(key, new FlowBean(sumUpFlow, sumDownFlow));
		}
	}

	public static void main(String[] args) throws Exception
	{
		if (null == args || args.length == 0)
		{
			throw new RuntimeException("args 参数为空");
		}

		Configuration config = new Configuration();
		Job job = Job.getInstance(config);

		job.setJarByClass(FlowSum.class);

		job.setMapperClass(FlowSumMapper.class);
		job.setReducerClass(FlowSumReducer.class);

		job.setPartitionerClass(ProvincePartitioner.class);
		job.setNumReduceTasks(4);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean flag = job.waitForCompletion(true);
		System.exit(flag ? 0 : 1);
	}
}
