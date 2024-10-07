package com.mawen.learn.rocketmq.common.message;

import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/1
 */
public class MessageAccessor {

	public static void clearProperty(final Message msg, final String name) {
		msg.clearProperty(name);
	}

	public static void setProperties(final Message msg, Map<String, String> properties) {
		msg.setProperties(properties);
	}

	public static void setTransFlag(final Message msg, String unit) {
		putProperty(msg, MessageConst.PROPERTY_TRANSFER_FLAG, unit);
	}

	public static void putProperty(final Message msg, final String name, final String value) {
		msg.putProperty(name, value);
	}

	public static String getTransFlag(final Message msg) {
		return msg.getProperty(MessageConst.PROPERTY_TRANSFER_FLAG);
	}

	public static void setCorrectionFlag(final Message msg, String unit) {
		putProperty(msg, MessageConst.PROPERTY_CORRECTION_FLAG, unit);
	}

	public static String getCorrectionFlag(final Message msg) {
		return msg.getProperty(MessageConst.PROPERTY_CORRECTION_FLAG);
	}

	public static void setOriginMessageId(final Message msg, String originMessageId) {
		putProperty(msg, MessageConst.PROPERTY_ORIGIN_MESSAGE_ID, originMessageId);
	}

	public static String getOriginMessageId(final Message msg) {
		return msg.getProperty(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID);
	}

	public static void setMQ2Flag(final Message msg, String flag) {
		putProperty(msg, MessageConst.PROPERTY_MQ2_FLAG, flag);
	}

	public static String getMQ2Flag(final Message msg) {
		return msg.getProperty(MessageConst.PROPERTY_MQ2_FLAG);
	}

	public static void setReconsumeTime(final Message msg, String reconsumeTime) {
		putProperty(msg, MessageConst.PROPERTY_RECONSUME_TIME, reconsumeTime);
	}

	public static String getReconsumeTime(final Message msg) {
		return msg.getProperty(MessageConst.PROPERTY_RECONSUME_TIME);
	}

	public static void setMaxReconsumeTimes(final Message msg, String maxReconsumeTime) {
		putProperty(msg, MessageConst.PROPERTY_MAX_RECONSUME_TIMES, maxReconsumeTime);
	}

	public static String getMaxReconsumeTimes(final Message msg) {
		return msg.getProperty(MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
	}

	public static void setConsumeStartTimeStamp(final Message msg, String propertyConsumeStartTimeStamp) {
		putProperty(msg, MessageConst.PROPERTY_CONSUME_START_TIMESTAMP, propertyConsumeStartTimeStamp);
	}

	public static String getConsumeStartTimeStamp(final Message msg) {
		return msg.getProperty(MessageConst.PROPERTY_CONSUME_START_TIMESTAMP);
	}

	public static Message cloneMessage(final Message msg) {
		Message newMsg = new Message(msg.getTopic(), msg.getBody());
		newMsg.setFlag(msg.getFlag());
		newMsg.setProperties(msg.getProperties());
		return newMsg;
	}

	public static Map<String, String> deepCopyProperties(Map<String, String> properties) {
		if (properties == null) {
			return null;
		}
		return new HashMap<>(properties);
	}
}
