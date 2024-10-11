package com.mawen.learn.rocketmq.remoting.protocol;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public enum SerializeType {
	JSON((byte) 0),
	ROCKETMQ((byte) 1);

	private final byte code;

	SerializeType(byte code) {
		this.code = code;
	}

	public byte getCode() {
		return code;
	}

	public static SerializeType valueOf(byte code) {
		for (SerializeType serializeType : SerializeType.values()) {
			if (serializeType.getCode() == code) {
				return serializeType;
			}
		}
		return null;
	}
}
