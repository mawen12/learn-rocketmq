package com.mawen.learn.rocketmq.remoting.protocol.heartbeat;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public enum MessageModel {
	BROADCASTING("BROADCASTING"),

	CLUSTERING("CLUSTERING");

	private final String modeCN;

	MessageModel(String modeCN) {
		this.modeCN = modeCN;
	}

	public String getModeCN() {
		return modeCN;
	}
}
