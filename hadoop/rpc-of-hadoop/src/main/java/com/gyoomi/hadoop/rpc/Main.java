/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

/**
 * 测试 - 调用客户端
 *
 * @author Leon
 * @date 2020-08-12 14:41
 */
public class Main
{

	public static void main(String[] args) throws Exception
	{
		LoginServiceInterface proxy = RPC.getProxy(LoginServiceInterface.class, 1L, new InetSocketAddress("localhost", 13144), new Configuration());
		String login = proxy.login("jack", "哈哈哈哈");
		System.out.println(login);

		ClientNameNodeProtocol proxy2 = RPC.getProxy(ClientNameNodeProtocol.class, 100L, new InetSocketAddress("localhost", 13145), new Configuration());
		String metaData = proxy2.getMetaData("/test");
		System.out.println(metaData);
	}

}
