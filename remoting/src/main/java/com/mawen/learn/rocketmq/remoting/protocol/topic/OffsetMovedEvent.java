package com.mawen.learn.rocketmq.remoting.protocol.topic;

import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public class OffsetMovedEvent extends RemotingSerializable {

	private String consumerGroup;

	private MessageQueue messageQueue;

	private long offsetRequest;

	private long offsetNew;

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}

	public MessageQueue getMessageQueue() {
		return messageQueue;
	}

	public void setMessageQueue(MessageQueue messageQueue) {
		this.messageQueue = messageQueue;
	}

	public long getOffsetRequest() {
		return offsetRequest;
	}

	public void setOffsetRequest(long offsetRequest) {
		this.offsetRequest = offsetRequest;
	}

	public long getOffsetNew() {
		return offsetNew;
	}

	public void setOffsetNew(long offsetNew) {
		this.offsetNew = offsetNew;
	}

	@Override
	public String toString() {
		return "OffsetMovedEvent [consumerGroup=" + consumerGroup + ", messageQueue=" + messageQueue
				+ ", offsetRequest=" + offsetRequest + ", offsetNew=" + offsetNew + "]";
	}
}
