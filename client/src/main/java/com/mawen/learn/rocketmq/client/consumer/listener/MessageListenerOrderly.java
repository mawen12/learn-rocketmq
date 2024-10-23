package com.mawen.learn.rocketmq.client.consumer.listener;

import java.util.List;

import com.mawen.learn.rocketmq.common.message.MessageExt;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/23
 */
public interface MessageListenerOrderly extends MessageListener{

	ConsumerOrderlyStatus consumeMessage(final List<MessageExt> msgs, final ConsumerOrderlyContext context);
}
