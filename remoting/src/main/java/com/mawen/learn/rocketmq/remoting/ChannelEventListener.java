package com.mawen.learn.rocketmq.remoting;

import io.netty.channel.Channel;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public interface ChannelEventListener {

	void onChannelConnect(final String remoteAddr, final Channel channel);

	void onChannelClose(final String remoteAddr, final Channel channel);

	void onChannelException(final String remoteAddr, final Channel channel);

	void onChannelIdle(final String remoteAddr, final Channel channel);

	void onChannelActive(final String remoteAddr, final Channel channel);
}
