package com.mawen.learn.rocketmq.client.consumer;

import com.mawen.learn.rocketmq.client.consumer.listener.MessageListener;
import com.mawen.learn.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.mawen.learn.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.mawen.learn.rocketmq.client.exception.MQClientException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
public interface MQPushConsumer extends MQConsumer {

	void start() throws MQClientException;

	void shutdown();

	void registerMessageListener(MessageListener messageListener);

	void registerMessageListener(final MessageListenerConcurrently messageListener);

	void registerMessageListener(final MessageListenerOrderly messageListener);

	void subscribe(final String topic, final String subExpression) throws MQClientException;

	void subscribe(final String topic, final String fullClassName, final String filterClassName) throws MQClientException;

	void subscribe(final String topic, final MessageSelector selector) throws MQClientException;

	void unsubscribe(final String topic);

	void updateCorePoolSize(int corePoolSize);

	void suspend();

	void resume();
}
