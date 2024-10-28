package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.List;
import java.util.Set;

import com.mawen.learn.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.mawen.learn.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.mawen.learn.rocketmq.client.consumer.MessageQueueListener;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.MessageModel;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/28
 */
public class RebalancePullImpl extends RebalanceImpl {

	private final DefaultMQPullConsumerImpl defaultMQPullConsumerImpl;

	public RebalancePullImpl(DefaultMQPullConsumerImpl defaultMQPullConsumerImpl) {
		this(null, null, null, null, defaultMQPullConsumerImpl);
	}

	public RebalancePullImpl(String consumerGroup, MessageModel messageModel, AllocateMessageQueueStrategy allocateMessageQueueStrategy, MQClientInstance mqClientFactory, DefaultMQPullConsumerImpl defaultMQPullConsumerImpl) {
		super(consumerGroup, messageModel, allocateMessageQueueStrategy, mqClientFactory);
		this.defaultMQPullConsumerImpl = defaultMQPullConsumerImpl;
	}

	@Override
	public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
		MessageQueueListener messageQueueListener = this.defaultMQPullConsumerImpl.getDefaultMQPullConsumer().getMessageQueueListener();
		if (messageQueueListener != null) {
			try {
				messageQueueListener.messageQueueChanged(topic, mqAll, mqDivided);
			}
			catch (Throwable e) {
				log.error("messageQueueChanged exception", e);
			}
		}
	}

	@Override
	public boolean removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq) {
		this.defaultMQPullConsumerImpl.getOffsetStore().persist(mq);
		this.defaultMQPullConsumerImpl.getOffsetStore().removeOffset(mq);
		return true;
	}

	@Override
	public ConsumeType consumeType() {
		return ConsumeType.CONSUME_ACTIVELY;
	}

	@Override
	public void removeDirtyOffset(MessageQueue mq) {
		this.defaultMQPullConsumerImpl.getOffsetStore().removeOffset(mq);
	}

	@Override
	public long computePullFromWhere(MessageQueue mq) {
		return 0;
	}

	@Override
	public long computePullFromWhereWithException(MessageQueue mq) throws MQClientException {
		return 0;
	}

	@Override
	public int getConsumeInitMode() {
		throw new UnsupportedOperationException("no initMode for pull");
	}

	@Override
	public void dispatchPullRequest(List<PullRequest> pullRequestList, long delay) {
		// NOP
	}

	@Override
	public void dispatchPopPullRequest(List<PopRequest> pullRequestList, long delay) {
		// NOP
	}

	@Override
	public ProcessQueue createProcessQueue() {
		return new ProcessQueue();
	}

	@Override
	public PopProcessQueue createPopProcessQueue() {
		return null;
	}
}
