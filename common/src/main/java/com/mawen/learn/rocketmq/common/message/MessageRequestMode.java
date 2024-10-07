package com.mawen.learn.rocketmq.common.message;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/1
 */
public enum MessageRequestMode {
	PULL("PULL"),
	POP("POP");

	private final String name;

	MessageRequestMode(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}
}
