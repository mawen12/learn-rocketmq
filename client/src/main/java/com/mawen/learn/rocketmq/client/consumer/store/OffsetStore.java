package com.mawen.learn.rocketmq.client.consumer.store;

import java.util.Map;
import java.util.Set;

import com.mawen.learn.rocketmq.client.exception.MQBrokerException;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.exception.RemotingException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/23
 */
public interface OffsetStore {

	void load() throws MQClientException;

	void updateOffset(final MessageQueue mq, final long offset, final boolean increaseOnly);

	void updateAndFreezeOffset(final MessageQueue mq, final long offset);

	long readOffset(final MessageQueue mq, final ReadOffsetType type);

	void removeOffset(MessageQueue mq);

	Map<MessageQueue, Long> cloneOffsetTable(String topic);

	void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

	void persist(MessageQueue mq);

	void persistAll(final Set<MessageQueue> mqs);
}
