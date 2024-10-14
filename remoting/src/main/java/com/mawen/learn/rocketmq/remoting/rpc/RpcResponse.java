package com.mawen.learn.rocketmq.remoting.rpc;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public class RpcResponse {

	private int code;

	private CommandCustomHeader header;

	private Object body;

	private RpcException exception;

	public RpcResponse() {
	}

	public RpcResponse(int code, CommandCustomHeader header, Object body) {
		this.code = code;
		this.header = header;
		this.body = body;
	}

	public RpcResponse(RpcException exception) {
		this.exception = exception;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public CommandCustomHeader getHeader() {
		return header;
	}

	public void setHeader(CommandCustomHeader header) {
		this.header = header;
	}

	public Object getBody() {
		return body;
	}

	public void setBody(Object body) {
		this.body = body;
	}

	public RpcException getException() {
		return exception;
	}

	public void setException(RpcException exception) {
		this.exception = exception;
	}
}
