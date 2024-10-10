package com.mawen.learn.rocketmq.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson2.annotation.JSONField;
import com.mawen.learn.rocketmq.common.attribute.TopicMessageType;
import com.mawen.learn.rocketmq.common.constant.PermName;

import static com.mawen.learn.rocketmq.common.TopicAttributes.*;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class TopicConfig {

	private static final String SEPARATOR = " ";

	private static final TypeReference<Map<String, String>> ATTRIBUTES_TYPE_REFERENCE = new TypeReference<Map<String, String>>(){};
	public static int defaultReadQueueNums = 16;
	public static int defaultWriteQueueNums = 16;

	private String topicName;
	private int readQueueNums = defaultReadQueueNums;
	private int writeQueueNums = defaultWriteQueueNums;
	private int perm = PermName.PERM_READ | PermName.PERM_WRITE;
	private TopicFilterType topicFilterType = TopicFilterType.SINGLE_FLAG;
	private int topicSysFlag = 0;
	private boolean order = false;
	private Map<String, String> attributes = new HashMap<>();

	public TopicConfig() {}

	public TopicConfig(String topicName) {
		this.topicName = topicName;
	}

	public TopicConfig(String topicName, int readQueueNums, int writeQueueNums) {
		this.topicName = topicName;
		this.readQueueNums = readQueueNums;
		this.writeQueueNums = writeQueueNums;
	}

	public TopicConfig(String topicName, int readQueueNums, int writeQueueNums, int perm, int topicSysFlag) {
		this.topicName = topicName;
		this.readQueueNums = readQueueNums;
		this.writeQueueNums = writeQueueNums;
		this.perm = perm;
		this.topicSysFlag = topicSysFlag;
	}

	public TopicConfig(TopicConfig other) {
		this.topicName = other.topicName;
		this.readQueueNums = other.readQueueNums;
		this.writeQueueNums = other.writeQueueNums;
		this.perm = other.perm;
		this.topicFilterType = other.topicFilterType;
		this.topicSysFlag = other.topicSysFlag;
		this.order = other.order;
		this.attributes = other.attributes;
	}

	public String encode() {
		StringBuilder sb = new StringBuilder();

		sb.append(this.topicName);
		sb.append(SEPARATOR);

		sb.append(this.readQueueNums);
		sb.append(SEPARATOR);

		sb.append(this.writeQueueNums);
		sb.append(SEPARATOR);

		sb.append(this.perm);
		sb.append(SEPARATOR);

		sb.append(this.topicFilterType);
		sb.append(SEPARATOR);

		if (attributes != null) {
			sb.append(JSON.toJSONString(attributes));
		}

		return sb.toString();
	}

	public boolean decode(final String in) {
		String[] strs = in.split(SEPARATOR);
		if (strs.length >= 5) {
			this.topicName = strs[0];
			this.readQueueNums = Integer.parseInt(strs[1]);
			this.writeQueueNums = Integer.parseInt(strs[2]);
			this.perm = Integer.parseInt(strs[3]);
			this.topicFilterType = TopicFilterType.valueOf(strs[4]);

			if (strs.length == 6) {
				try {
					this.attributes = JSON.parseObject(strs[5], ATTRIBUTES_TYPE_REFERENCE.getType());
				}
				catch (Exception ignored) {
				}
			}
			return true;
		}
		return false;
	}

	@JSONField(serialize = false, deserialize = false)
	public TopicMessageType getTopicMessageType() {
		if (attributes == null) {
			return TopicMessageType.NORMAL;
		}

		String content = attributes.get(TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName());
		if (content == null) {
			return TopicMessageType.NORMAL;
		}
		return TopicMessageType.valueOf(content);
	}

	@JSONField(serialize = false, deserialize = false)
	public void setTopicMessageType(TopicMessageType topicMessageType) {
		attributes.put(TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName(), topicMessageType.getValue());
	}

	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

	public int getReadQueueNums() {
		return readQueueNums;
	}

	public void setReadQueueNums(int readQueueNums) {
		this.readQueueNums = readQueueNums;
	}

	public int getWriteQueueNums() {
		return writeQueueNums;
	}

	public void setWriteQueueNums(int writeQueueNums) {
		this.writeQueueNums = writeQueueNums;
	}

	public int getPerm() {
		return perm;
	}

	public void setPerm(int perm) {
		this.perm = perm;
	}

	public TopicFilterType getTopicFilterType() {
		return topicFilterType;
	}

	public void setTopicFilterType(TopicFilterType topicFilterType) {
		this.topicFilterType = topicFilterType;
	}

	public int getTopicSysFlag() {
		return topicSysFlag;
	}

	public void setTopicSysFlag(int topicSysFlag) {
		this.topicSysFlag = topicSysFlag;
	}

	public boolean isOrder() {
		return order;
	}

	public void setOrder(boolean order) {
		this.order = order;
	}

	public Map<String, String> getAttributes() {
		return attributes;
	}

	public void setAttributes(Map<String, String> attributes) {
		this.attributes = attributes;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		TopicConfig that = (TopicConfig) o;

		if (readQueueNums != that.readQueueNums) {
			return false;
		}
		if (writeQueueNums != that.writeQueueNums) {
			return false;
		}
		if (perm != that.perm) {
			return false;
		}
		if (topicSysFlag != that.topicSysFlag) {
			return false;
		}
		if (order != that.order) {
			return false;
		}
		if (!Objects.equals(topicName, that.topicName)) {
			return false;
		}
		if (topicFilterType != that.topicFilterType) {
			return false;
		}
		return Objects.equals(attributes, that.attributes);
	}

	@Override
	public int hashCode() {
		int result = topicName != null ? topicName.hashCode() : 0;
		result = 31 * result + readQueueNums;
		result = 31 * result + writeQueueNums;
		result = 31 * result + perm;
		result = 31 * result + (topicFilterType != null ? topicFilterType.hashCode() : 0);
		result = 31 * result + topicSysFlag;
		result = 31 * result + (order ? 1 : 0);
		result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "TopicConfig [topicName=" + topicName + ", readQueueNums=" + readQueueNums
				+ ", writeQueueNums=" + writeQueueNums + ", perm=" + PermName.perm2String(perm)
				+ ", topicFilterType=" + topicFilterType + ", topicSysFlag=" + topicSysFlag + ", order=" + order
				+ ", attributes=" + attributes + "]";
	}
}
