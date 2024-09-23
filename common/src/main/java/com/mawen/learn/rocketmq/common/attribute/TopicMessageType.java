package com.mawen.learn.rocketmq.common.attribute;

import java.util.Map;

import com.mawen.learn.rocketmq.common.message.MessageConst;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
public enum TopicMessageType {
	UNSPECIFIED("UNSPECIFIED"),
	NORMAL("NORMAL"),
	FIFO("FIFO"),
	DELAY("DELAY"),
	TRANSACTION("TRANSACTION"),
	MIXED("MIXED"),
	;

	private final String value;

	TopicMessageType(String value) {
		this.value = value;
	}

	public static TopicMessageType parseFromMessageProperty(Map<String, String> messageProperty) {
		String isTrans = messageProperty.get(MessageConst.PROPERTY_TRANSACTION_PREPARED);
		String isTransValue = "true";

		if (isTransValue.equals(isTrans)) {
			return TopicMessageType.TRANSACTION;
		}
		else if (messageProperty.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL) != null
		         || messageProperty.get(MessageConst.PROPERTY_TIMER_DELAY_MS) != null
		         || messageProperty.get(MessageConst.PROPERTY_TIMER_DELAY_SEC) != null
		         || messageProperty.get(MessageConst.PROPERTY_TIMER_DELAY_MS) != null) {
			return TopicMessageType.DELAY;
		}
		else if (messageProperty.get(MessageConst.PROPERTY_SHARDING_KEY) != null) {
			return TopicMessageType.FIFO;
		}
		return TopicMessageType.NORMAL;
	}

	public String getValue() {
		return value;
	}

	public String getMetricsValue() {
		return value.toLowerCase();
	}

}
