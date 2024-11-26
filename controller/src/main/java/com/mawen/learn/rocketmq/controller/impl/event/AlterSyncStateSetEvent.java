package com.mawen.learn.rocketmq.controller.impl.event;

import java.util.Set;

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
public class AlterSyncStateSetEvent implements EventMessage{

	private final String brokerName;
	private final Set<Long> newSyncStateSet;

	@Override
	public EventType getEventType() {
		return EventType.ALTER_SYNC_STATE_SET_EVENT;
	}
}
