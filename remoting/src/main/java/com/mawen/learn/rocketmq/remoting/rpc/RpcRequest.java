package com.mawen.learn.rocketmq.remoting.rpc;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public class RpcRequest {

	int code;

	private RpcRequestHeader header;

	private Object body;

	public RpcRequest(int code, RpcRequestHeader header, Object body) {
		this.code = code;
		this.header = header;
		this.body = body;
	}

	public int getCode() {
		return code;
	}

	public RpcRequestHeader getHeader() {
		return header;
	}

	public Object getBody() {
		return body;
	}
}
