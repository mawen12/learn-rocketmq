package com.mawen.learn.rocketmq.controller.impl.event;

import com.mawen.learn.rocketmq.common.utils.FastJsonSerializer;

/**
 *
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/27
 */
public class EventSerializer {
	private final FastJsonSerializer serializer;

	public EventSerializer() {
		this.serializer = new FastJsonSerializer();
	}

	private void putShort(byte[] memory, int index, int value) {
		memory[index] = (byte) (value >>> 8);
		memory[index + 1] = (byte) value;
	}

	private short getShort(byte[] memory, int index) {
		return (short) (memory[index] << 8 | memory[index + 1] & 0xFF);
	}

	public byte[] serialize(EventMessage message) {
		short eventType = message.getEventType().getId();
		byte[] data = serializer.serialize(message);
		if (data != null && data.length > 0) {
			byte[] result = new byte[2 + data.length];
			putShort(result, 0, eventType);
			System.arraycopy(data, 0, result, 2, data.length);
			return result;
		}
		return null;
	}

	public EventMessage deserialize(byte[] bytes) {
		if (bytes.length < 2) {
			return null;
		}

		short eventId = getShort(bytes, 0);
		if (eventId > 0) {
			byte[] data = new byte[bytes.length - 2];
			System.arraycopy(bytes, 2, data, 0, data.length);
			EventType eventType = EventType.from(eventId);
			if (eventType != null) {
				switch (eventType) {
					case ALTER_SYNC_STATE_SET_EVENT:
						return serializer.deserialize(data, AlterSyncStateSetEvent.class);
					case APPLY_BROKER_ID_EVENT:
						return serializer.deserialize(data, ApplyBrokerIdEvent.class);
					case ELECT_MASTER_EVENT:
						return serializer.deserialize(data, ElectMasterEvent.class);
					case CLEAN_BROKER_DATA_EVENT:
						return serializer.deserialize(data, CleanBrokerDataEvent.class);
					case UPDATE_BROKER_ADDRESS:
						return serializer.deserialize(data, UpdateBrokerAddresssEvent.class);
					default:
						break;
				}
			}
		}
		return null;
	}
}
