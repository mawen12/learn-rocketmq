package com.mawen.learn.rocketmq.controller;

import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.remoting.ChannelEventListener;
import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/26
 */
@AllArgsConstructor
public class BrokerHousekeepingService implements ChannelEventListener {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

	private final ControllerManager controllerManager;

	@Override
	public void onChannelConnect(String remoteAddr, Channel channel) {
		// NOP
	}

	@Override
	public void onChannelClose(String remoteAddr, Channel channel) {
		controllerManager.getHeartbeatManager().onBrokerChannelClose(channel);
	}

	@Override
	public void onChannelException(String remoteAddr, Channel channel) {
		controllerManager.getHeartbeatManager().onBrokerChannelClose(channel);
	}

	@Override
	public void onChannelIdle(String remoteAddr, Channel channel) {
		controllerManager.getHeartbeatManager().onBrokerChannelClose(channel);
	}

	@Override
	public void onChannelActive(String remoteAddr, Channel channel) {
		// NOP
	}
}
