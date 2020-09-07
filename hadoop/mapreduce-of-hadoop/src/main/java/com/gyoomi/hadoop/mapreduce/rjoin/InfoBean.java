/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.mapreduce.rjoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Right join
 *
 * @author Leon
 * @date 2020-09-04 9:48
 */
public class InfoBean implements WritableComparable<InfoBean>
{

	private int orderId;
	private String orderDate;
	private String productId;
	private int amount;
	private String productName;
	private String categoryId;
	private float price;

	/**
	 * 	0 - 表示这个对象是封装订单表记录; 1 - 表示这个对象是封装产品信息记录
	 */
	private String flag;

	public InfoBean() {}

	public void set(int orderId, String orderDate, String productId, int amount, String productName, String categoryId, float price, String flag)
	{
		this.orderId = orderId;
		this.orderDate = orderDate;
		this.productId = productId;
		this.amount = amount;
		this.productName = productName;
		this.categoryId = categoryId;
		this.price = price;
		this.flag = flag;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException
	{
		dataOutput.writeInt(this.orderId);
		dataOutput.writeUTF(this.orderDate);
		dataOutput.writeUTF(this.productId);
		dataOutput.writeInt(this.amount);
		dataOutput.writeUTF(this.productName);
		dataOutput.writeUTF(this.categoryId);
		dataOutput.writeFloat(this.price);
		dataOutput.writeUTF(this.flag);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException
	{
		this.orderId = dataInput.readInt();
		this.orderDate = dataInput.readUTF();
		this.productId = dataInput.readUTF();
		this.amount = dataInput.readInt();
		this.productName = dataInput.readUTF();
		this.categoryId = dataInput.readUTF();
		this.price = dataInput.readFloat();
		this.flag = dataInput.readUTF();
	}

	public int getOrderId() {
		return orderId;
	}

	public void setOrderId(int orderId) {
		this.orderId = orderId;
	}

	public String getOrderDate() {
		return orderDate;
	}

	public void setOrderDate(String orderDate) {
		this.orderDate = orderDate;
	}

	public String getProductId() {
		return productId;
	}

	public void setProductId(String productId) {
		this.productId = productId;
	}

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public String getProductName() {
		return productName;
	}

	public void setProductName(String productName) {
		this.productName = productName;
	}

	public String getCategoryId() {
		return categoryId;
	}

	public void setCategoryId(String categoryId) {
		this.categoryId = categoryId;
	}

	public float getPrice() {
		return price;
	}

	public void setPrice(float price) {
		this.price = price;
	}

	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

	@Override
	public String toString()
	{
		return orderId + " " +  orderDate + " " + productId + " " +  amount + " " +  productName + " " + categoryId + " " + price + " " + flag;
	}

	@Override
	public int compareTo(InfoBean o)
	{
		return this.productId.compareTo(o.getProductId());
	}
}
