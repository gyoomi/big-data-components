/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.mapreduce.fans;

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
import java.util.Arrays;

/**
 * 共同好友 - 2
 *
 * @author Leon
 * @date 2020-09-15 17:47
 */
public class SharedFriendsStepTwo
{

	static class SharedFriendsStepTwoMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			// 拿到的数据是上一个步骤的输出结果
			// A I,K,C,B,G,F,H,O,D,
			// 友 人，人，人
			String lineText = value.toString();
			String[] friendPersons = lineText.split("\t");
			String friend = friendPersons[0];
			String[] persons = friendPersons[1].split(",");
			Arrays.sort(persons);

			for (int i = 0; i < persons.length - 1; i++)
			{
				for (int j = i + 1; j < persons.length; j++)
				{
					// 发出 <人-人，好友> ，这样，相同的“人-人”对的所有好友就会到同1个reduce中去
					context.write(new Text(persons[i] + "-" + persons[j]), new Text(friend));
				}
			}
		}
	}

	static class SharedFriendsStepTwoReducer extends Reducer<Text, Text, Text, Text>
	{

		@Override
		protected void reduce(Text personToPerson, Iterable<Text> friends, Context context) throws IOException, InterruptedException
		{
			StringBuilder sb = new StringBuilder();

			for (Text friend : friends)
			{
				sb.append(friend).append(" ");

			}
			context.write(personToPerson, new Text(sb.toString()));
		}
	}

	public static void main(String[] args) throws Exception
	{
		if (args.length <= 0)
		{
			args = new String[]{"D:/bigdata/fansoutput", "D:/bigdata/fansoutput222"};
		}

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(SharedFriendsStepTwo.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(SharedFriendsStepTwoMapper.class);
		job.setReducerClass(SharedFriendsStepTwoReducer.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

	}
}
