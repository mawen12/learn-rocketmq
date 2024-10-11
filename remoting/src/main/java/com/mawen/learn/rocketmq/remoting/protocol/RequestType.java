package com.mawen.learn.rocketmq.remoting.protocol;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public enum RequestType {
	STREAM((byte) 0);

	private final byte code;

	RequestType(byte code) {
		this.code = code;
	}

	public byte getCode() {
		return code;
	}

	public static RequestType valueOf(byte code) {
		for (RequestType requestType : RequestType.values()) {
			if (requestType.getCode() == code) {
				return requestType;
			}
		}
		return null;
	}
}
