package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.List;
import java.util.Set;

import com.mawen.learn.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.mawen.learn.rocketmq.client.consumer.DefaultLitePullConsumer;
import com.mawen.learn.rocketmq.client.consumer.MessageQueueListener;
import com.mawen.learn.rocketmq.client.consumer.store.ReadOffsetType;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.consumer.ConsumeFromWhere;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.MessageModel;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/28
 */
public class RebalanceLitePullImpl extends RebalanceImpl {

	private final DefaultLitePullConsumerImpl litePullConsumerImpl;

	public RebalanceLitePullImpl(DefaultLitePullConsumerImpl litePullConsumerImpl) {
		this(null, null, null, null, litePullConsumerImpl);
	}

	public RebalanceLitePullImpl(String consumerGroup, MessageModel messageModel, AllocateMessageQueueStrategy allocateMessageQueueStrategy, MQClientInstance mqClientFactory, DefaultLitePullConsumerImpl litePullConsumerImpl) {
		super(consumerGroup, messageModel, allocateMessageQueueStrategy, mqClientFactory);
		this.litePullConsumerImpl = litePullConsumerImpl;
	}

	@Override
	public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
		MessageQueueListener messageQueueListener = this.litePullConsumerImpl.getDefaultLitePullConsume().getMessagerQueueListener();
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
		this.litePullConsumerImpl.getOffsetStore().persist(mq);
		this.litePullConsumerImpl.getOffsetStore().removeOffset(mq);
		return true;
	}

	@Override
	public ConsumeType consumeType() {
		return ConsumeType.CONSUME_ACTIVELY;
	}

	@Override
	public void removeDirtyOffset(MessageQueue mq) {
		this.litePullConsumerImpl.getOffsetStore().removeOffset(mq);
	}

	@Override
	public long computePullFromWhere(MessageQueue mq) {
		long result = -1L;
		try {
			result = computePullFromWhereWithException(mq);
		}
		catch (MQClientException e) {
			log.warn("Compute consume offset exception, mq={}", mq, e);
		}
		return result;
	}

	@Override
	public long computePullFromWhereWithException(MessageQueue mq) throws MQClientException {
		ConsumeFromWhere consumeFromWhere = litePullConsumerImpl.getDefaultLitePulConsumer().getConsumeFromWhere();
		long result = -1;
		switch (consumeFromWhere) {
			case CONSUME_FROM_LAST_OFFSET: {
				long lastOffset = litePullConsumerImpl.getOffsetStore().readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
				if (lastOffset >= 0) {
					result = lastOffset;
				}
				else if (lastOffset == -1) {
					if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
						result = 0L;
					}
					else {
						try {
							result = this.mqClientFactory.getMqAdminImpl().maxOffset(mq);
						}
						catch (MQClientException e) {
							log.warn("Compute consume offset from last offset exception, mq={}, exception={}", mq, e);
						}
					}
				}
				else {
					result = -1;
				}
				break;
			}
			case CONSUME_FROM_FIRST_OFFSET: {
				long lastOffset = litePullConsumerImpl.getOffsetStore().readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
				if (lastOffset >= 0) {
					result = lastOffset;
				}
				else if (lastOffset == -1) {
					result = 0L;
				}
				else {
					result = -1;
				}
				break;
			}
			case CONSUME_FROM_TIMESTAMP: {
				long lastOffset = litePullConsumerImpl.getOffsetStore().readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
				if (lastOffset >= 0) {
					result = lastOffset;
				}
				else if (lastOffset == -1) {
					if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
						try {
							result = this.mqClientFactory.getMqAdminImpl().maxOffset(mq);
						}
						catch (MQClientException e) {
							log.warn("Compute consume offset from last offset exception, mq={}, exception={}", mq, e);
							throw e;
						}
					}
					else {
						try {
							long timestamp = UtilAll.parseDate(litePullConsumerImpl.getDefaultLitePullConsumer().getConsumeTimestamp(), UtilAll.YYYYMMDDHHMMSS).getTime();
							result = this.mqClientFactory.getMqAdminImpl().searchOffset(mq, timestamp);
						}
						catch (MQClientException e) {
							log.warn("Compute consume offset from last offset exception, mq={}, exception={}", mq, e);
							throw e;
						}
					}
				}
				else {
					result = -1;
				}
				break;
			}

		}
		return result;
	}

	@Override
	public int getConsumeInitMode() {
		throw new UnsupportedOperationException("no initMode for null");
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
