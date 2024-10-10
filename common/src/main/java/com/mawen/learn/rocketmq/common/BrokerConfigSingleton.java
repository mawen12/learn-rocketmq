package com.mawen.learn.rocketmq.common;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class BrokerConfigSingleton {

	private static AtomicBoolean isInit = new AtomicBoolean();

	private static BrokerConfig brokerConfig;

	public static BrokerConfig getBrokerConfig() {
		if (brokerConfig == null) {
			throw new IllegalArgumentException("brokerConfig Cannot be null!");
		}
		return brokerConfig;
	}

	public static void setBrokerConfig(BrokerConfig brokerConfig) {
		if (!isInit.compareAndSet(false, true)) {
			throw new IllegalArgumentException("broker config have inited!");
		}
		BrokerConfigSingleton.brokerConfig = brokerConfig;
	}
}
