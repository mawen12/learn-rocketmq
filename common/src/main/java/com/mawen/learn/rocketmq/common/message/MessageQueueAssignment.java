package com.mawen.learn.rocketmq.common.message;

import java.io.Serializable;
import java.util.Map;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/6
 */
public class MessageQueueAssignment implements Serializable {

	private static final long serialVersionUID = 6646951611528546178L;

	private MessageQueue messageQueue;

	private MessageRequestMode mode = MessageRequestMode.PULL;

	private Map<String, String> attachments;

	public MessageQueue getMessageQueue() {
		return messageQueue;
	}

	public void setMessageQueue(MessageQueue messageQueue) {
		this.messageQueue = messageQueue;
	}

	public MessageRequestMode getMode() {
		return mode;
	}

	public void setMode(MessageRequestMode mode) {
		this.mode = mode;
	}

	public Map<String, String> getAttachments() {
		return attachments;
	}

	public void setAttachments(Map<String, String> attachments) {
		this.attachments = attachments;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((messageQueue == null) ? 0 : messageQueue.hashCode());
		result = prime * result + ((mode == null) ? 0 : mode.hashCode());
		result = prime * result + ((attachments == null) ? 0 : attachments.hashCode());
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
		MessageQueueAssignment other = (MessageQueueAssignment) obj;
		return messageQueue.equals(other.messageQueue);
	}

	@Override
	public String toString() {
		return "MessageQueueAssignment [MessageQueue=" + messageQueue + ", Mode=" + mode + "]";
	}
}
