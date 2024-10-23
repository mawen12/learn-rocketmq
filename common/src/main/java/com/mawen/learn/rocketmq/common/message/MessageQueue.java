package com.mawen.learn.rocketmq.common.message;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/1
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class MessageQueue implements Comparable<MessageQueue>, Serializable {

	private static final long serialVersionUID = -8084174795288467394L;

	private String topic;
	private String brokerName;
	private int queueId;

	public MessageQueue(MessageQueue other) {
		this.topic = other.topic;
		this.brokerName = other.brokerName;
		this.queueId = other.queueId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
		result = prime * result + queueId;
		result = prime * result + ((topic == null) ? 0 : topic.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MessageQueue other = (MessageQueue) obj;
		if (brokerName == null) {
			if (other.brokerName != null)
				return false;
		} else if (!brokerName.equals(other.brokerName))
			return false;
		if (queueId != other.queueId)
			return false;
		if (topic == null) {
			if (other.topic != null)
				return false;
		} else if (!topic.equals(other.topic))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "MessageQueue [topic=" + topic + ", brokerName=" + brokerName + ", queueId=" + queueId + "]";
	}

	@Override
	public int compareTo(MessageQueue o) {
		{
			int result = this.topic.compareTo(o.topic);
			if (result != 0) {
				return result;
			}
		}

		{
			int result = this.brokerName.compareTo(o.brokerName);
			if (result != 0) {
				return result;
			}
		}

		return this.queueId - o.queueId;
	}
}
