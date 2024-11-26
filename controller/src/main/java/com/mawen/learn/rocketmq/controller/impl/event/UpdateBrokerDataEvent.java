package com.mawen.learn.rocketmq.controller.impl.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/26
 */
@Getter
@Setter
@ToString
@AllArgsConstructor
public class UpdateBrokerDataEvent implements EventMessage {

	private String clusterName;

	private String brokerName;

	private String brokerAddress;

	private Long brokerId;

	@Override
	public EventType getEventType() {
		return EventType.UPDATE_BROKER_ADDRESS;
	}
}
