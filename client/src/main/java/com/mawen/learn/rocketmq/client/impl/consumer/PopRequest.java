package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.Objects;

import com.mawen.learn.rocketmq.common.constant.ConsumeInitMode;
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
public class PopRequest implements MessageRequest{

	private String topic;
	private String consumerGroup;
	private MessageQueue messageQueue;
	private PopProcessQueue popProcessQueue;
	private boolean lockedFirst = false;
	private int initMode = ConsumeInitMode.MAX;

	@Override
	public MessageRequestMode getMessageRequestMode() {
		return MessageRequestMode.POP;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PopRequest that = (PopRequest) o;
		return Objects.equals(topic, that.topic) && Objects.equals(consumerGroup, that.consumerGroup) && Objects.equals(messageQueue, that.messageQueue);
	}

	@Override
	public int hashCode() {
		return Objects.hash(topic, consumerGroup, messageQueue);
	}

	@Override
	public String toString() {
		return "PopRequest{" +
				"topic='" + topic + '\'' +
				", consumerGroup='" + consumerGroup + '\'' +
				", messageQueue=" + messageQueue +
				'}';
	}
}
