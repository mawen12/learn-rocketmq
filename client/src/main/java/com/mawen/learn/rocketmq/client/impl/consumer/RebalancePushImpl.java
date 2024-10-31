package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.mawen.learn.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.mawen.learn.rocketmq.client.consumer.MessageQueueListener;
import com.mawen.learn.rocketmq.client.consumer.store.OffsetStore;
import com.mawen.learn.rocketmq.client.consumer.store.ReadOffsetType;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.constant.ConsumeInitMode;
import com.mawen.learn.rocketmq.common.consumer.ConsumeFromWhere;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.MessageModel;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/28
 */
public class RebalancePushImpl extends RebalanceImpl {

	private static final long UNLOCK_DELAY_TIME_MILLIS = Long.parseLong(System.getProperty("rocketmq.client.unlockDelayTimeMillis", "20000"));

	private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

	public RebalancePushImpl(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
		this(null, null, null, null, defaultMQPushConsumerImpl);
	}

	public RebalancePushImpl(String consumerGroup, MessageModel messageModel, AllocateMessageQueueStrategy allocateMessageQueueStrategy,
			MQClientInstance mqClientFactory, DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
		super(consumerGroup, messageModel, allocateMessageQueueStrategy, mqClientFactory);
		this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
	}

	@Override
	public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
		SubscriptionData subscriptionData = this.subscriptionInner.get(topic);
		long newVersion = System.currentTimeMillis();
		log.info("{} Rebalance changed, also update version: {}, {}", topic, subscriptionData.getSubVersion(), newVersion);
		subscriptionData.setSubVersion(newVersion);

		int currentQueueCount = this.processQueueTable.size();
		if (currentQueueCount != 0) {
			int pullThresholdForTopic = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdSizeForTopic();
			if (pullThresholdForTopic != -1) {
				int newVal = Math.max(1, pullThresholdForTopic / currentQueueCount);
				log.info("The pullThresholdForQueue is changed from {} to {}", this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdForQueue(), newVal);
				this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().setPullThresholdForQueue(newVal);
			}

			int pulThresholdSizeForTopic = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdSizeForTopic();
			if (pulThresholdSizeForTopic != -1) {
				int newVal = Math.max(1, pulThresholdSizeForTopic / currentQueueCount);
				log.info("The pullThresholdSizeForQueue is changed from {} to {}", this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdSizeForQueue(), newVal);
				this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().setPullThresholdSizeForQueue(newVal);
			}
		}

		this.getMqClientFactory().sendHeartbeartToAllBrokerWithLockV2(true);

		MessageQueueListener messageQueueListener = this.defaultMQPushConsumerImpl.getMessageQueueListener();
		if (messageQueueListener != null) {
			messageQueueListener.messageQueueChanged(topic, mqAll, mqDivided);
		}
	}

	@Override
	public boolean removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq) {
		if (this.defaultMQPushConsumerImpl.isConsumeOrderly() && MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
			this.defaultMQPushConsumerImpl.getOffsetStore().persist(mq);
			return tryRemoveOrderMessageQueue(mq, pq);
		}
		else {
			this.defaultMQPushConsumerImpl.getOffsetStore().persist(mq);
			this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);
			return true;
		}
	}

	@Override
	public ConsumeType consumeType() {
		return ConsumeType.CONSUME_PASSIVELY;
	}

	@Override
	public void removeDirtyOffset(MessageQueue mq) {
		this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);
	}

	@Override
	public long computePullFromWhere(MessageQueue mq) {
		long result = -1L;
		try {
			result = computePullFromWhereWithException(mq);
		}
		catch (MQClientException e) {
			log.warn("Compute consume offset exception, mq={}", mq);
		}
		return result;
	}

	@Override
	public long computePullFromWhereWithException(MessageQueue mq) throws MQClientException {
		long result = -1;
		final ConsumeFromWhere consumeFromWhere = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumerFromWhere();
		final OffsetStore offsetStore = this.defaultMQPushConsumerImpl.getOffsetStore();
		switch (consumeFromWhere) {
			case CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST:
			case CONSUME_FROM_MIN_OFFSET:
			case CONSUME_FROM_MAX_OFFSET:
			case CONSUME_FROM_LAST_OFFSET: {
				long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
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
							throw e;
						}
					}
				}
				else {
					throw new MQClientException(ResponseCode.QUERY_NOT_FOUND, "Failed to query consume offset from offset store");
				}
				break;
			}
			case CONSUME_FROM_FIRST_OFFSET: {
				long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
				if (lastOffset >= 0) {
					result = lastOffset;
				}
				else if (lastOffset == -1) {
					result = 0L;
				}
				else {
					throw new MQClientException(ResponseCode.QUERY_NOT_FOUND, "Failed to query offset from offset store");
				}
				break;
			}
			case CONSUME_FROM_TIMESTAMP: {
				long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
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
							long timestamp = UtilAll.parseDate(this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeTimestamp(), UtilAll.YYYYMMDDHHMMSS).getTime();
							result = this.mqClientFactory.getMqAdminImpl().searchOffset(mq, timestamp);
						}
						catch (MQClientException e) {
							log.warn("Compute consume offset from last offset exception, mq={}, exception={}", mq, e);
							throw e;
						}
					}
				}
				break;
			}
			default:
				break;
		}

		if (result < 0) {
			throw new MQClientException(ResponseCode.SYSTEM_ERROR, "Found unexpected result " + result);
		}

		return result;
	}

	@Override
	public boolean clientRebalance(String topic) {
		return this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().isClientRebalance()
				|| this.defaultMQPushConsumerImpl.isConsumeOrderly()
				|| MessageModel.BROADCASTING.equals(messageModel);
	}

	@Override
	public int getConsumeInitMode() {
		final ConsumeFromWhere consumeFromWhere = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeFromWhere();
		if (consumeFromWhere == ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET) {
			return ConsumeInitMode.MIN;
		}
		return ConsumeInitMode.MAX;
	}

	@Override
	public void dispatchPullRequest(List<PullRequest> pullRequestList, long delay) {
		for (PullRequest pullRequest : pullRequestList) {
			if (delay <= 0) {
				this.defaultMQPushConsumerImpl.executePullRequestImmediately(pullRequest);
			}
			else {
				this.defaultMQPushConsumerImpl.executePullRequestLater(pullRequest, delay);
			}
		}
	}

	@Override
	public void dispatchPopPullRequest(List<PopRequest> pullRequestList, long delay) {
		for (PopRequest pullRequest : pullRequestList) {
			if (delay <= 0) {
				this.defaultMQPushConsumerImpl.executePopPullRequestImmediately(pullRequest);
			}
			else {
				this.defaultMQPushConsumerImpl.executePopPullRequestLaster(pullRequest, delay);
			}
		}
	}

	@Override
	public ProcessQueue createProcessQueue() {
		return new ProcessQueue();
	}

	@Override
	public PopProcessQueue createPopProcessQueue() {
		return new PopProcessQueue();
	}

	private boolean tryRemoveOrderMessageQueue(final MessageQueue mq, final ProcessQueue pq) {
		try {
			boolean forceUnlock = pq.isDropped() && System.currentTimeMillis() > pq.getLastLockTimestamp() + UNLOCK_DELAY_TIME_MILLIS;
			if (forceUnlock || pq.getConsumeLock().writeLock().tryLock(500, TimeUnit.MILLISECONDS)) {
				try {
					this.defaultMQPushConsumerImpl.getOffsetStore().persist(mq);
					this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);

					pq.setLocked(false);
					this.unlock(mq, true);
					return true;
				}
				finally {
					if (!forceUnlock) {
						pq.getConsumeLock().writeLock().unlock();
					}
				}
			}
		}
		catch (Exception e) {
			pq.incTryUnlockTimes();
		}
		return false;
	}

}
