/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.rpc;

/**
 * The description of interface
 *
 * @author Leon
 * @date 2020-08-12 15:05
 */
public interface ClientNameNodeProtocol
{

	public static final long versionID = 100L;

	/**
	 * 获取指定路径的dfs元数据信息
	 * @param path 指定的路径
	 * @return 元数据
	 */
	String getMetaData(String path);

}
