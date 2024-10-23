package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.Objects;

import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.common.message.MessageRequestMode;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/23
 */
@Setter
@Getter
public class PullRequest implements MessageRequest {

	private String consumerGroup;
	private MessageQueue messageQueue;
	private ProcessQueue processQueue;
	private long nextOffset;
	private boolean previouslyLocked = false;

	@Override
	public MessageRequestMode getMessageRequestMode() {
		return MessageRequestMode.PULL;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PullRequest that = (PullRequest) o;
		return Objects.equals(consumerGroup, that.consumerGroup) && Objects.equals(messageQueue, that.messageQueue);
	}

	@Override
	public int hashCode() {
		return Objects.hash(consumerGroup, messageQueue);
	}

	@Override
	public String toString() {
		return "PullRequest{" +
				"consumerGroup='" + consumerGroup + '\'' +
				", messageQueue=" + messageQueue +
				", nextOffset=" + nextOffset +
				'}';
	}
}
