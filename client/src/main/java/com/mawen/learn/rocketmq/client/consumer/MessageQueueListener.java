package com.mawen.learn.rocketmq.client.consumer;

import java.util.Set;

import com.mawen.learn.rocketmq.common.message.MessageQueue;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
public interface MessageQueueListener {

	void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll, final Set<MessageQueue> mqAssigned);
}