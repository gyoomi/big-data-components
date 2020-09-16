/**
 * Copyright © 2020, Glodon Digital Supplier & Purchaser BU.
 * <p>
 * All Rights Reserved.
 */

package com.gyoomi.hadoop.mapreduce.logwash;

/**
 * log bean
 *
 * @author Leon
 * @date 2020-09-16 11:33
 */
public class WebLogBean
{

	/**
	 * 记录客户端的ip地址
	 */
	private String remoteAddr;

	/**
	 * 记录客户端用户名称,忽略属性"-"
	 */
	private String remoteUser;

	/**
	 * 记录访问时间与时区
	 */
	private String timeLocal;

	/**
	 * 记录请求的url与http协议
	 */
	private String request;

	/**
	 * 记录请求状态；成功是200
	 */
	private String status;

	/**
	 * 记录发送给客户端文件主体内容大小
	 */
	private String bodyBytesSent;

	/**
	 * 用来记录从那个页面链接访问过来的
	 */
	private String httpReferer;

	/**
	 * 记录客户浏览器的相关信息
	 */
	private String httpUserAgent;

	/**
	 * 判断数据是否合法
	 */
	private boolean valid = true;


	public String getRemoteAddr() {
		return remoteAddr;
	}

	public void setRemoteAddr(String remoteAddr) {
		this.remoteAddr = remoteAddr;
	}

	public String getRemoteUser() {
		return remoteUser;
	}

	public void setRemoteUser(String remoteUser) {
		this.remoteUser = remoteUser;
	}

	public String getTimeLocal() {
		return timeLocal;
	}

	public void setTimeLocal(String timeLocal) {
		this.timeLocal = timeLocal;
	}

	public String getRequest() {
		return request;
	}

	public void setRequest(String request) {
		this.request = request;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getBodyBytesSent() {
		return bodyBytesSent;
	}

	public void setBodyBytesSent(String bodyBytesSent) {
		this.bodyBytesSent = bodyBytesSent;
	}

	public String getHttpReferer() {
		return httpReferer;
	}

	public void setHttpReferer(String httpReferer) {
		this.httpReferer = httpReferer;
	}

	public String getHttpUserAgent() {
		return httpUserAgent;
	}

	public void setHttpUserAgent(String httpUserAgent) {
		this.httpUserAgent = httpUserAgent;
	}

	public boolean isValid() {
		return valid;
	}

	public void setValid(boolean valid) {
		this.valid = valid;
	}

	@Override
	public String toString()
	{

		String sb = this.valid +
			"\001" + this.remoteAddr +
			"\001" + this.remoteUser +
			"\001" + this.timeLocal +
			"\001" + this.request +
			"\001" + this.status +
			"\001" + this.bodyBytesSent +
			"\001" + this.httpReferer +
			"\001" + this.httpUserAgent;
		return sb;
	}
}
