package com.mawen.learn.rocketmq.controller.impl.event;

import java.util.Set;

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
public class CleanBrokerDataEvent implements EventMessage{

	private String brokerName;

	private Set<Long> brokerIdSetToClean;

	@Override
	public EventType getEventType() {
		return EventType.CLEAN_BROKER_DATA_EVENT;
	}

}
