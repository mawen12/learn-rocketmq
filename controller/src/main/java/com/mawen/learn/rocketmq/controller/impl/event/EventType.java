package com.mawen.learn.rocketmq.controller.impl.event;

import lombok.Getter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/26
 */
@Getter
public enum EventType {

	ALTER_SYNC_STATE_SET_EVENT("AlterSyncStateSetEvent", (short) 1),
	APPLY_BROKER_ID_EVENT("ApplyBrokerIdEvent", (short) 2),
	ELECT_MASTER_EVENT("ElectMasterEvent", (short) 3),
	READ_EVENT("ReadEvent", (short) 4),
	CLEAN_BROKER_DATA_EVENT("CleanBrokerDataEvent", (short) 5),

	UPDATE_BROKER_ADDRESS("UpdateBrokerAddressEvent", (short) 6);

	private final String name;
	private final short id;

	EventType(String name, short id) {
		this.name = name;
		this.id = id;
	}

	public static EventType from(short id) {
		switch (id) {
			case 1:
				return ALTER_SYNC_STATE_SET_EVENT;
			case 2:
				return APPLY_BROKER_ID_EVENT;
			case 3:
				return ELECT_MASTER_EVENT;
			case 4:
				return READ_EVENT;
			case 5:
				return CLEAN_BROKER_DATA_EVENT;
			case 6:
				return UPDATE_BROKER_ADDRESS;
		}
		return null;
	}
}
