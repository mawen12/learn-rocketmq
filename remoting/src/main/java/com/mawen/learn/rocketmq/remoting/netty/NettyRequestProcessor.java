package com.mawen.learn.rocketmq.remoting.netty;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public interface NettyRequestProcessor {

	RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception;

	boolean rejectRequest();
}
