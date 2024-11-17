package com.mawen.learn.rocketmq.store.plugin;

import com.mawen.learn.rocketmq.common.BrokerConfig;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.remoting.Configuration;
import com.mawen.learn.rocketmq.store.MessageArrivingListener;
import com.mawen.learn.rocketmq.store.config.MessageStoreConfig;
import com.mawen.learn.rocketmq.store.stats.BrokerStatsManager;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/17
 */
@Getter
@AllArgsConstructor
public class MessageStorePluginContext {
	private MessageStoreConfig messageStoreConfig;
	private BrokerStatsManager brokerStatsManager;
	private MessageArrivingListener messageArrivingListener;
	private BrokerConfig brokerConfig;
	private final Configuration configuration;

	public void registerConfiguration(Object config) {
		MixAll.properties2Object(configuration.getAllConfigs(),config);
		configuration.registerConfig(config);
	}
}
