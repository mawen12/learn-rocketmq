package com.mawen.learn.rocketmq.client.producer;

import java.util.List;

import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageQueue;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
public interface MessageQueueSelector {

	MessageQueue select(final List<MessageQueue> mqs, final Message msg, final Object arg);
}
