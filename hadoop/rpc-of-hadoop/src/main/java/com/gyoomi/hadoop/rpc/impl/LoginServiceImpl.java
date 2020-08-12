/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.rpc.impl;

import com.gyoomi.hadoop.rpc.LoginServiceInterface;

/**
 * 登录 - protocol - 实现
 *
 * @author Leon
 * @date 2020-08-11 16:09
 */
public class LoginServiceImpl implements LoginServiceInterface
{


	@Override
	public String login(String username, String password)
	{
		System.out.println(username + "你总算来了，等死我了。");
		return username + "successfully logon in , welcome!!!";
	}

}
