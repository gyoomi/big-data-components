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

/**
 * 共同好友 - 1
 *
 * @author Leon
 * @date 2020-09-15 17:01
 */
public class SharedFriendsStepOne
{

	static class SharedFriendsStepOneMapper extends Mapper<LongWritable, Text, Text, Text>
	{

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			// A:B,C,D,F,E,O
			// B:A,C,E,K
			// C:F,A,D,I
			String lineText = value.toString();
			String[] personFriends = lineText.split(":");
			String person = personFriends[0];
			String friends = personFriends[1];
			for (String friend : friends.split(","))
			{
				// 输出： <好友, 人>
				context.write(new Text(friend), new Text(person));
			}
		}
	}

	static class SharedFriendsStepOneReducer extends Reducer<Text, Text, Text, Text>
	{

		@Override
		protected void reduce(Text friend, Iterable<Text> persons, Context context) throws IOException, InterruptedException
		{
			StringBuilder stringBuilder = new StringBuilder();
			for (Text person : persons)
			{
				stringBuilder.append(person).append(",");
			}
			context.write(friend, new Text(stringBuilder.toString()));
		}

	}

	public static void main(String[] args) throws Exception
	{
		if (args.length <= 0)
		{
			args = new String[]{"D:/bigdata/fansinput", "D:/bigdata/fansoutput"};
		}
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(SharedFriendsStepOne.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(SharedFriendsStepOneMapper.class);
		job.setReducerClass(SharedFriendsStepOneReducer.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
