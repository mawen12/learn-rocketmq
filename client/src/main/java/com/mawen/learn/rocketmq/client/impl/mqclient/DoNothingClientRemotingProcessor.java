package com.mawen.learn.rocketmq.client.impl.mqclient;

import com.mawen.learn.rocketmq.client.impl.ClientRemotingProcessor;
import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/28
 */
public class DoNothingClientRemotingProcessor extends ClientRemotingProcessor {

	public DoNothingClientRemotingProcessor(MQClientInstance mqClientFactory) {
		super(mqClientFactory);
	}

	@Override
	public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
		return null;
	}
}
