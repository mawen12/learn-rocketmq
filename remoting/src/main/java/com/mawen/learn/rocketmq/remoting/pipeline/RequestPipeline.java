package com.mawen.learn.rocketmq.remoting.pipeline;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/15
 */
public interface RequestPipeline {

	void execute(ChannelHandlerContext ctx, RemotingCommand cmd) throws Exception;

	default RequestPipeline pipe(RequestPipeline source) {
		return (ctx, request) -> {
			source.execute(ctx, request);
			execute(ctx, request);
		};
	}
}
