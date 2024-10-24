package com.mawen.learn.rocketmq.client.consumer;

import java.util.Set;

import com.mawen.learn.rocketmq.client.exception.MQBrokerException;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
public interface MQPullConsumer extends MQConsumer {

	void start() throws MQClientException;

	void shutdown();

	void registerMessageQueueListener(final String topic, final MessageQueueListener listener);

	PullResult pull(final MessageQueue mq, final String subExpression, final long offset, final int maxNums) throws MQClientException, RuntimeException, MQBrokerException, InterruptedException;

	PullResult pull(final MessageQueue mq, final String subExpression, final long offset, final int maxNums, final long timeout) throws MQClientException, RuntimeException, MQBrokerException, InterruptedException;

	PullResult pull(final MessageQueue mq, final MessageSelector selector, final long offset, final int maxNums) throws MQClientException, RuntimeException, MQBrokerException, InterruptedException;

	PullResult pull(final MessageQueue mq, final MessageSelector selector, final long offset, final int maxNums, final long timeout) throws MQClientException, RuntimeException, MQBrokerException, InterruptedException;

	void pull(final MessageQueue mq, final String subExpression, final long offset, final int maxNums, final PullCallback callback) throws  MQClientException, RuntimeException, MQBrokerException, InterruptedException;

	void pull(final MessageQueue mq, final String subExpression, final long offset, final int maxNums, final PullCallback callback, final long timeout) throws  MQClientException, RuntimeException, MQBrokerException, InterruptedException;

	void pull(final MessageQueue mq, final String subExpression, final long offset, final int maxNums, final int maxSize, final PullCallback callback) throws  MQClientException, RuntimeException, MQBrokerException, InterruptedException;

	void pull(final MessageQueue mq, final String subExpression, final long offset, final int maxNums, final int maxSize, final PullCallback callback, final long timeout) throws  MQClientException, RuntimeException, MQBrokerException, InterruptedException;

	void pull(final MessageQueue mq, final MessageSelector selector, final long offset, final int maxNums, final int maxSize, final PullCallback callback) throws  MQClientException, RuntimeException, MQBrokerException, InterruptedException;

	void pull(final MessageQueue mq, final MessageSelector selector, final long offset, final int maxNums, final int maxSize, final PullCallback callback, final long timeout) throws  MQClientException, RuntimeException, MQBrokerException, InterruptedException;

	PullResult pullBlockIfNotFound(final MessageQueue mq, final String subExpression, final long offset, final int maxNums) throws  MQClientException, RuntimeException, MQBrokerException, InterruptedException;

	PullResult pullBlockIfNotFound(final MessageQueue mq, final String subExpression, final long offset, final int maxNums, final PullCallback callback) throws  MQClientException, RuntimeException, MQBrokerException, InterruptedException;

	PullResult pullBlockIfNotFoundWithMessageSelector(final MessageQueue mq, final MessageSelector selector, final long offset, final int maxNums) throws  MQClientException, RuntimeException, MQBrokerException, InterruptedException;

	PullResult pullBlockIfNotFoundWithMessageSelector(final MessageQueue mq, final MessageSelector selector, final long offset, final int maxNums, final PullCallback callback) throws  MQClientException, RuntimeException, MQBrokerException, InterruptedException;

	void updateConsumeOffset(final MessageQueue mq, final long offset) throws MQClientException;

	long fetchConsumeOffset(final MessageQueue mq, final boolean fromStore) throws MQClientException;

	Set<MessageQueue> fetchMessageQueuesInBalance(final String topic) throws MQClientException;

	void sendMessageBack(MessageExt msg, int delayLevel, String brokerName, String consumerGroup) throws  MQClientException, RuntimeException, MQBrokerException, InterruptedException;

}
