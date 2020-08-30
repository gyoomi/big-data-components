package com.gyoomi.hadoop.mapreduce.flowsum;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 流量统计 Bean
 *
 * @author Leon
 * @version 2020/8/30 16:19
 */
public class FlowBean implements Writable
{

	private long upFlow;

	private long downFlow;

	private long sumFlow;

	public FlowBean() {}

	public FlowBean(long upFlow, long downFlow)
	{
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}


	/**
	 * 序列化方法
	 * @param dataOutput
	 * @throws IOException
	 */
	@Override
	public void write(DataOutput dataOutput) throws IOException
	{
		dataOutput.writeLong(this.upFlow);
		dataOutput.writeLong(this.downFlow);
		dataOutput.writeLong(this.sumFlow);
	}

	/**
	 * 反序列化方法: 反序列化的顺序跟序列化的顺序完全一致
	 * @param dataInput
	 * @throws IOException
	 */
	@Override
	public void readFields(DataInput dataInput) throws IOException
	{
		this.upFlow = dataInput.readLong();
		this.downFlow = dataInput.readLong();
		this.sumFlow = dataInput.readLong();
	}

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + sumFlow;
	}
}
