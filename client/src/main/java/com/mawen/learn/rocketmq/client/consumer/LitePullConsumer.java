package com.mawen.learn.rocketmq.client.consumer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
public interface LitePullConsumer {

	void start() throws MQClientException;

	void shutdown();

	boolean isRunning();

	void subscribe(final String topic) throws MQClientException;

	void subscribe(final String topic, final String subExpression) throws MQClientException;

	void subscribe(final String topic, final String subExpression, final MessageQueueListener listener) throws MQClientException;

	void subscribe(final String topic, final MessageSelector selector) throws MQClientException;

	void unsubscribe(final String topic);

	Set<MessageQueue> assignment() throws MQClientException;

	void assign(Collection<MessageQueue> messageQueues);

	void setSubExpressionForAssign(final String topic, final String subExpression);

	List<MessageExt> poll();

	List<MessageExt> poll(long timeout);

	void seek(MessageQueue mq, long offset) throws MQClientException;

	void pause(Collection<MessageQueue> messageQueues);

	void resume(Collection<MessageQueue> messageQueues);

	boolean isAutoCommit();

	void setAutoCommit(boolean autoCommit);

	Collection<MessageQueue> fetchMessageQueues(String topic) throws MQClientException;

	Long offsetForTimestamp(MessageQueue mq, Long timestamp) throws MQClientException;

	void commitSync();

	void commitSync(Map<MessageQueue, Long> offsetMap, boolean persist);

	void commit();

	void commit(Map<MessageQueue, Long> offsetMap, boolean persist);

	void commit(final Set<MessageQueue> messageQueues, boolean persist);

	Long committed(MessageQueue mq) throws MQClientException;

	void registerTopicMessageQueueChangeListener(String topic, TopicMessageQueueChangeListener listener) throws MQClientException;

	void updateNameServerAddress(String nameServerAddress);

	void seekToBegin(MessageQueue mq) throws MQClientException;

	void seekToEnd(MessageQueue mq) throws MQClientException;
}
