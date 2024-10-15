package com.mawen.learn.rocketmq.remoting;

import com.mawen.learn.rocketmq.remoting.pipeline.RequestPipeline;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public interface RemotingService {

	void start();

	void shutdown();

	void registerRPCHook(RPCHook rpcHook);

	void clearRPCHook();

	void setRequestPipeline(RequestPipeline pipeline);
}
