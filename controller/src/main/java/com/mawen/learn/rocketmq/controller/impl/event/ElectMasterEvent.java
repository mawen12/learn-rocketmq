package com.mawen.learn.rocketmq.controller.impl.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/26
 */
@Getter
@ToString
@AllArgsConstructor
public class ElectMasterEvent implements EventMessage{

	private final boolean newMasterElectId;
	private final String brokerName;
	private final Long newMasterBrokerId;

	public ElectMasterEvent(boolean newMasterElectId, String brokerName) {
		this(newMasterElectId, brokerName, null);
	}

	public ElectMasterEvent(String brokerName, Long newMasterBrokerId) {
		this(true, brokerName, newMasterBrokerId);
	}

	@Override
	public EventType getEventType() {
		return EventType.ELECT_MASTER_EVENT;
	}

}
