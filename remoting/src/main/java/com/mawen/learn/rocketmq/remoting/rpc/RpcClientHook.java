package com.mawen.learn.rocketmq.remoting.rpc;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public abstract class RpcClientHook {

	public abstract RpcResponse beforeRequest(RpcRequest request) throws RpcException;

	public abstract RpcResponse afterResponse(RpcResponse response) throws RpcException;
}
