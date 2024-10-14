package com.mawen.learn.rocketmq.remoting.rpc;

import com.mawen.learn.rocketmq.remoting.exception.RemotingException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public class RpcException extends RemotingException {

	private int errorCode;

	public RpcException(int errorCode, String message) {
		super(message);
		this.errorCode = errorCode;
	}

	public RpcException(int errorCode, String message, Throwable cause) {
		super(message, cause);
		this.errorCode = errorCode;
	}

	public int getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(int errorCode) {
		this.errorCode = errorCode;
	}
}
