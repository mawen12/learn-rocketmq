package com.mawen.learn.rocketmq.controller.helper;

import com.mawen.learn.rocketmq.controller.impl.heartbeat.BrokerLiveInfo;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/27
 */
public interface BrokerLiveInfoGetter {

	BrokerLiveInfo get(String clusterName, String brokerName, Long brokerId);
}
