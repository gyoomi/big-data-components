/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.mapreduce.mapsidejoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
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
		protected void setup(Context context) throws IOException, InterruptedException
		{
			//获取缓存文件路径
			URI[] cacheFiles = context.getCacheFiles();
			URI cacheFileUri = cacheFiles[0];
			String path = cacheFileUri.toURL().getPath();
			System.out.println("==================================");
			System.out.println(path);
			System.out.println("==================================");
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
			String productLineValue;
			while (StringUtils.isNotBlank(productLineValue = reader.readLine()))
			{
				String[] fields = productLineValue.split(",");
				productCache.put(fields[0], fields[1]);
			}
			reader.close();
		}

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

	public static void main(String[] args) throws Exception
	{
		Configuration configuration = new Configuration();

		Job job = Job.getInstance(configuration);
		job.setJarByClass(MapSideJoin.class);
		job.setMapperClass(MapSideJoinMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path("D:/bigdata/mapjoininput"));
		FileOutputFormat.setOutputPath(job, new Path("D:/bigdata/mapjoinoutput"));
		// 指定需要缓存一个文件到所有的maptask运行节点工作目录
		/* job.addArchiveToClassPath(archive); */// 缓存jar包到task运行节点的classpath中
		/* job.addFileToClassPath(file); */// 缓存普通文件到task运行节点的classpath中
		/* job.addCacheArchive(uri); */// 缓存压缩包文件到task运行节点的工作目录
		/* job.addCacheFile(uri) */// 缓存普通文件到task运行节点的工作目录

		job.addCacheFile(new URI("file:/D:/bigdata/pdts.txt"));

		// map端join的逻辑不需要reduce阶段，设置reducetask数量为0
		job.setNumReduceTasks(0);
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
		System.out.println("over");
	}

}
