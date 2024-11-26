package com.mawen.learn.rocketmq.controller;

import java.nio.channels.Channel;
import java.util.Map;

import com.mawen.learn.rocketmq.common.ControllerConfig;
import com.mawen.learn.rocketmq.controller.helper.BrokerLifecycleListener;
import com.mawen.learn.rocketmq.controller.impl.heartbeat.BrokerLiveInfo;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/26
 */
public interface BrokerHeartbeatManger {
	long DEFAULT_BROKER_CHANNEL_EXPIRED_TIME = 10 * 1000;

	static BrokerHeartbeatManger newBrokerHeartbeatManager(ControllerConfig controllerConfig) {
		if (controllerConfig.getControllerType().equals(ControllerConfig.JRAFT_CONTROLLER)) {
			return new RaftBrokerHeartBeatManager(controllerConfig);
		}
		else {
			return new DefaultBrokerHeartbeatManager(controllerConfig);
		}
	}

	void initialize();

	void onBrokerHeartbeat(final String clusterName, final String brokerName, final String brokerAddr,
	                       final Long brokerId, final Long timeoutMillis, final Channel channel, final Integer epoch,
	                       final Long maxOffset, final Long confirmOffset, final Integer electionPriority);

	void start();

	void shutdown();

	void registerBrokerLifecycleListener(final BrokerLifecycleListener listener);

	void onBrokerChannelClose(final Channel channel);

	BrokerLiveInfo getBrokerLiveInfo(String clusterName, String brokerName, Long brokerId);

	boolean isBrokerActive(final String clusterName, final String brokerName, final Long brokerId);

	Map<String, Map<String, Integer>> getActiveBrokersNum();
}
