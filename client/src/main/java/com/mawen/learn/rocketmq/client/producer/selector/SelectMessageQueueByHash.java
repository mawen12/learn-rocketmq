package com.mawen.learn.rocketmq.client.producer.selector;

import java.util.List;

import com.mawen.learn.rocketmq.client.producer.MessageQueueSelector;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageQueue;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
public class SelectMessageQueueByHash implements MessageQueueSelector {

	@Override
	public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
		int value = arg.hashCode() % mqs.size();
		if (value < 0) {
			value = Math.abs(value);
		}
		return mqs.get(value);
	}
}
