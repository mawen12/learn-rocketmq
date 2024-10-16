package com.mawen.learn.rocketmq.client.producer.selector;

import java.util.List;
import java.util.Set;

import com.mawen.learn.rocketmq.client.producer.MessageQueueSelector;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
@Getter
@Setter
public class SelectMessageQueueByMachineRoom implements MessageQueueSelector {

	private Set<String> consumeridcs;

	@Override
	public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
		return null;
	}
}
