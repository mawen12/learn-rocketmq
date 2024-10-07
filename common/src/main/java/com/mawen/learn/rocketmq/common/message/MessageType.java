package com.mawen.learn.rocketmq.common.message;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/1
 */
public enum MessageType {
	Normal_Msg("Normal"),
	Trans_Msg_Half("Trans"),
	Trans_Msg_Commit("TransCommit"),
	Delay_Msg("Delay"),
	Order_Msg("Order");

	private final String shortName;

	MessageType(String shortName) {
		this.shortName = shortName;
	}

	public String getShortName() {
		return shortName;
	}

	public static MessageType getByShortName(String shortName) {
		for (MessageType msgType : MessageType.values()) {
			if (msgType.getShortName().equals(shortName)) {
				return msgType;
			}
		}
		return Normal_Msg;
	}
}
