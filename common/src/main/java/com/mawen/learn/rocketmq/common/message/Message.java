package com.mawen.learn.rocketmq.common.message;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/1
 */
public class Message implements Serializable {

	private static final long serialVersionUID = -797360904586996160L;

	private String topic;
	private int flag;
	private Map<String, String> properties;
	private byte[] body;
	private String transactionId;

	public Message() {}

	public Message(String topic, byte[] body) {
		this(topic, "", "", 0, body, true);
	}

	public Message(String topic, String tags, String keys, int flag, byte[] body, boolean waitStoreMsgOK) {
		this.topic = topic;
		this.flag = flag;
		this.body = body;

		if (tags != null && tags.length() > 0) {
			this.setTags(tags);
		}

		if (keys != null && keys.length() > 0) {
			this.setKeys(keys);
		}

		this.setWaitStoreMsgOK(waitStoreMsgOK);
	}

	public Message(String topic, String tags, byte[] body) {
		this(topic, tags, "", 0, body, true);
	}

	public Message(String topic, String tags, String keys, byte[] body) {
		this(topic, tags, keys, 0, body, true);
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getTags() {
		return this.getProperty(MessageConst.PROPERTY_TAGS);
	}

	public void setTags(String tags) {
		this.putProperty(MessageConst.PROPERTY_TAGS, tags);
	}

	public String getKeys() {
		return this.getProperty(MessageConst.PROPERTY_KEYS);
	}

	public void setKeys(Collection<String> keyCollection) {
		String keys = String.join(MessageConst.KEY_SEPARATOR, keyCollection);

		this.setKeys(keys);
	}

	public void setKeys(String keys) {
		this.putProperty(MessageConst.PROPERTY_KEYS, keys);
	}

	public int getDelayTimeLevel() {
		String t = this.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
		if (t != null) {
			return Integer.parseInt(t);
		}
		return 0;
	}

	public void setDelayTimeLevel(int level) {
		this.putProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL, String.valueOf(level));
	}

	public boolean isWaitStoreMsgOK() {
		String result = this.getProperty(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
		if (result != null) {
			return Boolean.parseBoolean(result);
		}
		return true;
	}

	public void setWaitStoreMsgOK(boolean waitStoreMsgOK) {
		this.putProperty(MessageConst.PROPERTY_WAIT_STORE_MSG_OK,Boolean.toString(waitStoreMsgOK));
	}

	public void setInstanceId(String instanceId) {
		this.putProperty(MessageConst.PROPERTY_INSTANCE_ID, instanceId);
	}

	public int getFlag() {
		return flag;
	}

	public void setFlag(int flag) {
		this.flag = flag;
	}

	public byte[] getBody() {
		return body;
	}

	public void setBody(byte[] body) {
		this.body = body;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}

	public String getBuyerId() {
		return this.getProperty(MessageConst.PROPERTY_BUYER_ID);
	}

	public void setBuyerId(String buyerId) {
		this.putProperty(MessageConst.PROPERTY_BUYER_ID,buyerId);
	}

	public String getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}

	public void setDelayTimeSec(long sec) {
		this.putProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC,String.valueOf(sec));
	}

	public long getDelayTimeSec() {
		String t = this.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC);
		if (t != null) {
			return Long.parseLong(t);
		}
		return 0L;
	}

	public void setDelayTimeMs(long timeMs) {
		this.putProperty(MessageConst.PROPERTY_TIMER_DELAY_MS, String.valueOf(timeMs));
	}

	public long getDeliverTimeMs() {
		String t = this.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS);
		if (t != null) {
			return Long.parseLong(t);
		}
		return 0;
	}

	public String getUserProperty(final String name) {
		return this.getProperty(name);
	}

	public void putUserProperty(final String name, final String value) {
		if (MessageConst.STRING_HASH_SET.contains(name)) {
			throw new RuntimeException(String.format("This property<%s> is used by system, input another please", name));
		}

		if (value == null || value.trim().isEmpty() || name == null || name.trim().isEmpty()) {
			throw new IllegalArgumentException("The name or value of property can not be null or blank string!");
		}

		this.putProperty(name,value);
	}

	void putProperty(final String name, final String value) {
		if (this.properties == null) {
			this.properties = new HashMap<>();
		}

		this.properties.put(name, value);
	}

	void clearProperty(final String name) {
		if (this.properties != null) {
			this.properties.remove(name);
		}
	}

	public String getProperty(final String name) {
		if (this.properties == null) {
			this.properties = new HashMap<>();
		}

		return this.properties.get(name);
	}

	@Override
	public String toString() {
		return "Message{" +
		       "topic='" + topic + '\'' +
		       ", flag=" + flag +
		       ", properties=" + properties +
		       ", body=" + Arrays.toString(body) +
		       ", transactionId='" + transactionId + '\'' +
		       '}';
	}
}
