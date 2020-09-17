/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.mapreduce.logenhancer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *  maptask或者reducetask在最终输出时，先调用OutputFormat的getRecordWriter方法拿到一个RecordWriter
 *  然后再调用RecordWriter的write(k,v)方法将数据写出
 *
 * @author Leon
 * @date 2020-09-17 17:11
 */
public class LogEnhancerOutputFormat extends FileOutputFormat<Text, NullWritable>
{


	@Override
	public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException
	{
		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path enhancerPath = new Path("d:/bigdata/en/log.dat");
		Path crawlPath = new Path("d:/bigdata/crawl/crawl.log.dat");

		FSDataOutputStream enhancerOs = fs.create(enhancerPath);
		FSDataOutputStream crawlOs = fs.create(crawlPath);
		return new EnhanceRecordWriter(enhancerOs, crawlOs);
	}

	static class EnhanceRecordWriter extends RecordWriter<Text, NullWritable>
	{

		FSDataOutputStream enhancedOs = null;
		FSDataOutputStream tocrawlOs = null;

		public EnhanceRecordWriter(FSDataOutputStream enhancedOs, FSDataOutputStream tocrawlOs)
		{
			super();
			this.enhancedOs = enhancedOs;
			this.tocrawlOs = tocrawlOs;
		}

		@Override
		public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException
		{
			String lineText = text.toString();
			if (lineText.contains("crawl"))
			{
				// 如果要写出的数据是待爬的url，则写入待爬清单文件 /logenhance/tocrawl/url.dat
				tocrawlOs.write(lineText.getBytes());
			}
			else
			{
				// 如果要写出的数据是增强日志，则写入增强日志文件 /logenhance/enhancedlog/log.dat
				enhancedOs.write(lineText.getBytes());
			}
		}

		@Override
		public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
		{
			if (tocrawlOs != null) {
				tocrawlOs.close();
			}
			if (enhancedOs != null) {
				enhancedOs.close();
			}
		}
	}
}
