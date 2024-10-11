package com.mawen.learn.rocketmq.remoting.protocol;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public enum RequestSource {
	SDK(-1),
	PROXY_FOR_ORDER(0),
	PROXY_FOR_BROADCAST(1),
	PROXY_FOR_STREAM(2);

	public static final String SYSTEM_PROPERTY_KEY = "rocketmq.requestSource";

	private final int value;

	RequestSource(int value) {
		this.value = value;
	}

	public int getValue() {
		return value;
	}

	public static boolean isValid(Integer value) {
		return value != null && value >= -1 && value < RequestSource.values().length - 1;
	}

	public static RequestSource parseInteger(Integer value) {
		if (isValid(value)) {
			return RequestSource.values()[value + 1];
		}
		return SDK;
	}
}
