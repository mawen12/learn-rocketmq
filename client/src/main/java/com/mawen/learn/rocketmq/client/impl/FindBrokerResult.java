package com.mawen.learn.rocketmq.client.impl;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/23
 */
@Getter
@AllArgsConstructor
public class FindBrokerResult {
	private final String brokerAddr;
	private final boolean slave;
	private final int brokerVersion;

	public FindBrokerResult(String brokerAddr, boolean slave) {
		this(brokerAddr, slave, 0);
	}
}
