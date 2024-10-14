package com.mawen.learn.rocketmq.remoting.rpc;

import java.util.concurrent.Future;

import com.mawen.learn.rocketmq.common.message.MessageQueue;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public interface RpcClient {

	Future<RpcResponse> invoke(RpcRequest request, long timeoutMs) throws RpcException;

	Future<RpcResponse> invoke(MessageQueue mq, RpcRequest request, long timeoutMs) throws RpcException;
}
