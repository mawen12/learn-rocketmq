package com.mawen.learn.rocketmq.controller.helper;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/26
 */
public interface BrokerLifecycleListener {

	void onBrokerInactive(final String clusterName, final String brokerName, final Long brokerId);
}
