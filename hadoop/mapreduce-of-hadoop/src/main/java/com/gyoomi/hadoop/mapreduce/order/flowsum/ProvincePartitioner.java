package com.gyoomi.hadoop.mapreduce.order.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * K2  V2  对应的是map输出kv的类型
 *
 * @author Leon
 * @version 2020/8/31 23:16
 */
public class ProvincePartitioner extends Partitioner<FlowBean, Text>
{
	public static Map<String, Integer> provinceDict = new HashMap<>();

	static
	{
		provinceDict.put("136", 0);
		provinceDict.put("137", 1);
		provinceDict.put("138", 2);
		provinceDict.put("139", 3);
	}


	@Override
	public int getPartition(FlowBean flowBean, Text text, int i)
	{
		String mobilePhonePrefix = text.toString().substring(0, 3);
		return provinceDict.get(mobilePhonePrefix) == null ? 0 : provinceDict.get(mobilePhonePrefix);
	}
}
