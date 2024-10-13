package com.mawen.learn.rocketmq.remoting.protocol.heartbeat;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public enum ConsumeType {
	CONSUME_ACTIVELY("PULL"),

	CONSUME_PASSIVELY("PUSH"),

	CONSUME_POP("POP");

	private final String typeCN;

	ConsumeType(String typeCN) {
		this.typeCN = typeCN;
	}

	public String getTypeCN() {
		return typeCN;
	}
}
