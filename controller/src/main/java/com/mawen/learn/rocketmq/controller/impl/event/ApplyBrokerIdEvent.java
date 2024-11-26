package com.mawen.learn.rocketmq.controller.impl.event;

import lombok.Getter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/26
 */
@Getter
@ToString
public class ApplyBrokerIdEvent implements EventMessage {

	private final String clusterName;
	private final String brokerName;
	private final String brokerAddress;

	private final String registerCheckCode;

	private final long newBrokerId;

	public ApplyBrokerIdEvent(String clusterName, String brokerName, String brokerAddress, long newBrokerId, String registerCheckCode) {
		this.clusterName = clusterName;
		this.brokerName = brokerName;
		this.brokerAddress = brokerAddress;
		this.registerCheckCode = registerCheckCode;
		this.newBrokerId = newBrokerId;
	}

	@Override
	public EventType getEventType() {
		return EventType.APPLY_BROKER_ID_EVENT;
	}
}
