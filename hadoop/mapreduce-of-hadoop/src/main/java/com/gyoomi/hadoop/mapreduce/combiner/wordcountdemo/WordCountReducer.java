package com.gyoomi.hadoop.mapreduce.combiner.wordcountdemo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Word Count Reducer <br/>
 *
 * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *     KEYIN、VALUEIN: 对应的是mapper输出的KEYOUT,VALUEOUT类型
 *     KEYOUT, VALUEOUT 是自定义reduce逻辑处理结果的输出数据类型
 *     在这里：KEYOUT是单词；VLAUEOUT是总次数
 *
 * @author Leon
 * @version 2020/8/29 14:59
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{

	/**
	 * 	 <angelababy,1><angelababy,1><angelababy,1><angelababy,1><angelababy,1>
	 * 	 <hello,1><hello,1><hello,1><hello,1><hello,1><hello,1>
	 * 	 <banana,1><banana,1><banana,1><banana,1><banana,1><banana,1>
	 * 	 入参key，是一组相同单词kv对的key
	 *
	 * @param key
	 * @param values
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		AtomicInteger wordCounter = new AtomicInteger();
		for (IntWritable value : values)
		{
			// value not use
			wordCounter.getAndIncrement();
		}
		context.write(key, new IntWritable(wordCounter.get()));
	}

}
