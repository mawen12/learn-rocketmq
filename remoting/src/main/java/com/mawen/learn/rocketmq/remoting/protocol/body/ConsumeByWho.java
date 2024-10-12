package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.HashSet;
import java.util.Set;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class ConsumeByWho extends RemotingSerializable {

	private Set<String> consumedGroup = new HashSet<>();

	private Set<String> notConsumedGroup = new HashSet<>();

	private String topic;

	private int queueId;

	private long offset;

	public Set<String> getConsumedGroup() {
		return consumedGroup;
	}

	public void setConsumedGroup(Set<String> consumedGroup) {
		this.consumedGroup = consumedGroup;
	}

	public Set<String> getNotConsumedGroup() {
		return notConsumedGroup;
	}

	public void setNotConsumedGroup(Set<String> notConsumedGroup) {
		this.notConsumedGroup = notConsumedGroup;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getQueueId() {
		return queueId;
	}

	public void setQueueId(int queueId) {
		this.queueId = queueId;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}
}
