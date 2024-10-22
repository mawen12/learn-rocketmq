package com.mawen.learn.rocketmq.client.exception;

import com.mawen.learn.rocketmq.common.UtilAll;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
public class RequestTimeoutException extends Exception{

	private static final long serialVersionUID = -1030527431719138518L;

	private int responseCode;

	private String errorMessage;

	public RequestTimeoutException(String errorMessage, Throwable cause) {
		super(errorMessage, cause);
		this.responseCode = -1;
		this.errorMessage = errorMessage;
	}

	public RequestTimeoutException(int responseCode, String errorMessage) {
		super("CODE: " + UtilAll.responseCode2String(responseCode) + " DESC: " + errorMessage);
		this.responseCode = responseCode;
		this.errorMessage = errorMessage;
	}

	public int getResponseCode() {
		return responseCode;
	}

	public void setResponseCode(int responseCode) {
		this.responseCode = responseCode;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}
}
