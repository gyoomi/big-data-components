/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.rpc;

/**
 * 登录 - protocol
 *
 * @author Leon
 * @date 2020-08-11 16:06
 */
public interface LoginServiceInterface
{

	public static final long versionID = 1L;

	String login(String username, String password);

}
