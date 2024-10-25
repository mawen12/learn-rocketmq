package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.mawen.learn.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.mawen.learn.rocketmq.client.consumer.listener.ConsumeReturnType;
import com.mawen.learn.rocketmq.client.consumer.listener.ConsumerOrderlyContext;
import com.mawen.learn.rocketmq.client.consumer.listener.ConsumerOrderlyStatus;
import com.mawen.learn.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.mawen.learn.rocketmq.client.hook.ConsumeMessageContext;
import com.mawen.learn.rocketmq.client.stat.ConsumerStatsManager;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.ThreadFactoryImpl;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageAccessor;
import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.common.utils.ThreadUtils;
import com.mawen.learn.rocketmq.remoting.protocol.NamespaceUtil;
import com.mawen.learn.rocketmq.remoting.protocol.body.CMResult;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.MessageModel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
public class ConsumeMessageOrderlyService implements ConsumeMessageService {

	private static final Logger log = LoggerFactory.getLogger(ConsumeMessageOrderlyService.class);

	private static final long MAX_TIME_CONSUME_CONTINUOUSLY = Long.parseLong(System.getProperty("rocketmq.client.maxTimeConsumeContinuously", "60000"));

	private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
	private final DefaultMQPushConsumer defaultMQPushConsumer;
	private final MessageListenerOrderly messageListener;
	private final BlockingQueue<Runnable> consumeRequestQueue;
	private final ThreadPoolExecutor consumeExecutor;
	private final String consumerGroup;
	private final MessageQueueLock messageQueueLock = new MessageQueueLock();
	private final ScheduledExecutorService scheduledExecutorService;
	private volatile boolean stopped = false;

	public ConsumeMessageOrderlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl, MessageListenerOrderly messageListener) {
		this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
		this.messageListener = messageListener;

		this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
		this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
		this.consumeRequestQueue = new LinkedBlockingDeque<>();

		String consumerGroupTag = consumerGroup.length() > 100 ? consumerGroup.substring(0, 100) : consumerGroup + "_";
		this.consumeExecutor = new ThreadPoolExecutor(this.defaultMQPushConsumer.getConsumeThreadMin(), this.defaultMQPushConsumer.getConsumeThreadMax(), 60 * 1000, TimeUnit.MILLISECONDS, this.consumeRequestQueue, new ThreadFactoryImpl("ConsumeMessageThread_" + consumerGroupTag));

		this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_" + consumerGroupTag));
	}

	@Override
	public void start() {
		if (MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
			this.scheduledExecutorService.scheduleAtFixedRate(() -> {
				try {
					ConsumeMessageOrderlyService.this.lockMQPeriodically();
				}
				catch (Throwable e) {
					log.error("scheduleAtFixedRate lockMQPeriodically exception", e);
				}
			}, 1000, ProcessQueue.REBALANCE_LOCK_INTERVAL, TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public void shutdown(long awaitTerminateMillis) {
		this.stopped = true;
		this.scheduledExecutorService.shutdown();
		ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
		if (MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
			this.unlockAllMQ();
		}
	}

	public synchronized void unlockAllMQ() {
		this.defaultMQPushConsumerImpl.getRebalanceImpl().unlockAll(false);
	}

	@Override
	public void updateCorePoolSize(int corePoolSize) {
		if (corePoolSize > 0 && corePoolSize <= Short.MAX_VALUE && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
			this.consumeExecutor.setCorePoolSize(corePoolSize);
		}
	}

	@Override
	public void incCorePoolSize() {
		// NOP
	}

	@Override
	public void decCorePoolSize() {
		// NOP
	}

	@Override
	public int getCorePoolSize() {
		return this.consumeExecutor.getCorePoolSize();
	}

	@Override
	public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
		ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
		result.setOrder(true);

		List<MessageExt> msgs = Arrays.asList(msg);

		MessageQueue mq = new MessageQueue();
		mq.setBrokerName(brokerName);
		mq.setTopic(msg.getTopic());
		mq.setQueueId(msg.getQueueId());

		ConsumerOrderlyContext context = new ConsumerOrderlyContext(mq);

		this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msg, this.consumerGroup);

		final long begin = System.currentTimeMillis();
		log.info("consumeMessageDirectly receive new message: {}", msg);

		try {
			ConsumerOrderlyStatus status = this.messageListener.consumeMessage(msgs, context);
			if (status != null) {
				switch (status) {
					case COMMIT:
						result.setConsumeResult(CMResult.CR_COMMIT);
						break;
					case ROLLBACK:
						result.setConsumeResult(CMResult.CR_ROLLBACK);
						break;
					case SUCCESS:
						result.setConsumeResult(CMResult.CR_SUCCESS);
						break;
					case SUSPEND_CURRENT_QUEUE_A_MOMENT:
						result.setConsumeResult(CMResult.CR_LATER);
						break;
					default:
						break;
				}
			}
			else {
				result.setConsumeResult(CMResult.CR_RETURN_NULL);
			}
		}
		catch (Throwable e) {
			result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
			result.setRemark(UtilAll.exceptionSimpleDesc(e));

			log.warn("consumeMessageDirectly exception: {} group: {} Msgs: {} MQ: {}",
					result.getRemark(), this.consumerGroup, msg, mq, e);
		}

		result.setAutoCommit(context.isAutoCommit());
		result.setSpentTimeMillis(System.currentTimeMillis() - begin);

		log.info("consumeMessageDirectly Result: {}", result);

		return result;
	}

	@Override
	public void submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue, boolean dispathToConsume) {
		if (dispathToConsume) {
			ConsumeRequest request = new ConsumeRequest(processQueue, messageQueue);
			this.consumeExecutor.submit(request);
		}
	}

	@Override
	public void submitPopConsumeRequest(List<MessageExt> msgs, PopProcessQueue popProcessQueue, MessageQueue messageQueue) {
		throw new UnsupportedOperationException();
	}



	public synchronized void lockMQPeriodically() {
		if (!this.stopped) {
			this.defaultMQPushConsumerImpl.getRebalanceImpl().lockAll();
		}
	}

	public void tryLockLaterAndReconsume(final MessageQueue mq, final ProcessQueue processQueue, final long delayMillis) {
		this.scheduledExecutorService.schedule(() -> {
			boolean lockOK = this.lockOneMQ(mq);
			if (lockOK) {
				submitConsumeRequestLater(processQueue, mq, 10);
			}
			else {
				submitConsumeRequestLater(processQueue, mq, 3000);
			}
		}, delayMillis, TimeUnit.MILLISECONDS);
	}

	public synchronized boolean lockOneMQ(final MessageQueue mq) {
		if (!this.stopped) {
			return this.defaultMQPushConsumerImpl.getRebalanceImpl().lock(mq);
		}
		return false;
	}

	public boolean processConsumeResult(final List<MessageExt> msgs, final ConsumerOrderlyStatus status, final ConsumerOrderlyContext context, final ConsumeRequest consumeRequest) {
		boolean continueConsume = false;
		long commitOffset = -1L;
		if (context.isAutoCommit()) {
			switch (status) {
				case COMMIT:
				case ROLLBACK:
					log.warn("the message queue consume result is illegal, we think you want to ack these message {}", consumeRequest.getMessageQueue());
				case SUCCESS:
					commitOffset = consumeRequest.getProcessQueue().commit();
					this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
					break;
				case SUSPEND_CURRENT_QUEUE_A_MOMENT:
					this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
					if (checkReconsumeTimes(msgs)) {
						consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);
						this.submitConsumeRequestLater(consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue(), context.getSuspendCurrentQueueTimeMillis());
						continueConsume = false;
					}
					else {
						commitOffset = consumeRequest.getProcessQueue().commit();
					}
					break;
				default:
					break;
			}
		}
		else {
			switch (status) {
				case SUCCESS:
					this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
					break;
				case COMMIT:
					commitOffset = consumeRequest.getProcessQueue().commit();
					break;
				case ROLLBACK:
					consumeRequest.getProcessQueue().rollback();
					this.submitConsumeRequestLater(consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue(), context.getSuspendCurrentQueueTimeMillis());
					continueConsume = false;
					break;
				case SUSPEND_CURRENT_QUEUE_A_MOMENT:
					this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
					if (checkReconsumeTimes(msgs)) {
						consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);
						this.submitConsumeRequestLater(consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue(), context.getSuspendCurrentQueueTimeMillis());
						continueConsume = false;
					}
					break;
				default:
					break;
			}
		}

		if (commitOffset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
			this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), commitOffset, false);
		}
		return continueConsume;
	}

	private void submitConsumeRequestLater(final ProcessQueue processQueue, final MessageQueue messageQueue, final long suspendTimeMillis) {
		long timeMillis = suspendTimeMillis;
		if (timeMillis == -1) {
			timeMillis = this.defaultMQPushConsumer.getSuspendCurrentQueueTimeMillis();
		}

		if (timeMillis < 10) {
			timeMillis = 10;
		}
		else if (timeMillis > 30000) {
			timeMillis = 30000;
		}

		this.scheduledExecutorService.schedule(() -> {
			submitConsumeRequest(null, processQueue, messageQueue, true);
		}, timeMillis, TimeUnit.MILLISECONDS);
	}

	public ConsumerStatsManager getConsumerStatsManager() {
		return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
	}

	public boolean sendMessageBack(final MessageExt msg) {
		try {
			Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());
			MessageAccessor.setProperties(newMsg, msg.getProperties());

			String originMsgId = MessageAccessor.getOriginMessageId(msg);
			MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);

			newMsg.setFlag(msg.getFlag());

			MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
			MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes() + 1));
			MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
			MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);
			newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

			this.defaultMQPushConsumerImpl.getMQClientFactory().getDefaultMQProducer().send(newMsg);
			return true;
		}
		catch (Exception e) {
			log.error("sendMessageBack exception, group: {}, msg: {}", this.consumerGroup, msg, e);
		}

		return false;
	}

	public void resetNamespace(final List<MessageExt> msgs) {
		for (MessageExt msg : msgs) {
			if (StringUtils.isNotEmpty(defaultMQPushConsumer.getNamespace())) {
				msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), defaultMQPushConsumer.getNamespace()));
			}
		}
	}

	private int getMaxReconsumeTimes() {
		if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
			return Integer.MAX_VALUE;
		}
		else {
			return this.defaultMQPushConsumer.getMaxReconsumeTimes();
		}
	}

	private boolean checkReconsumeTimes(List<MessageExt> msgs) {
		boolean suspend = false;
		if (msgs != null && !msgs.isEmpty()) {
			for (MessageExt msg : msgs) {
				if (msg.getReconsumeTimes() >= getMaxReconsumeTimes()) {
					MessageAccessor.setReconsumeTime(msg, String.valueOf(msg.getReconsumeTimes()));
					if (!sendMessageBack(msg)) {
						suspend = true;
						msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
					}
				}
				else {
					suspend = true;
					msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
				}
			}
		}
		return suspend;
	}

	@Getter
	@RequiredArgsConstructor
	class ConsumeRequest implements Runnable {

		private final ProcessQueue processQueue;
		private final MessageQueue messageQueue;

		@Override
		public void run() {
			if (this.processQueue.isDropped()) {
				log.warn("run, the message queue not able to be consume, because it's dropped. {}", this.messageQueue);
				return;
			}

			Object objLock = messageQueueLock.fetchLockObject(this.messageQueue);
			synchronized (objLock) {
				if (MessageModel.BROADCASTING.equals(defaultMQPushConsumerImpl.messageModel()) || this.processQueue.isLocked() && !this.processQueue.isLockExpired()) {
					final long beginTime = System.currentTimeMillis();
					for (boolean continueConsume = true; continueConsume; ) {
						if (this.processQueue.isDropped()) {
							log.warn("run, the message queue not able to be consume, because it's dropped. {}", this.messageQueue);
							break;
						}

						if (MessageModel.CLUSTERING.equals(defaultMQPushConsumerImpl.messageModel()) && !this.processQueue.isLocked()) {
							log.warn("the message queue not locked, so consume later, {}", this.messageQueue);
							tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
							break;
						}

						if (MessageModel.CLUSTERING.equals(defaultMQPushConsumerImpl.messageModel()) && !this.processQueue.isLockExpired()) {
							log.warn("the message queue lock expired, so consume later, {}", this,messageListener);
							tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
							break;
						}

						long interval = System.currentTimeMillis() - beginTime;
						if (interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
							submitConsumeRequestLater(this.processQueue, this.messageQueue, 10);
							break;
						}

						final int consumeBatchSize = defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
						List<MessageExt> msgs = this.processQueue.takeMessages(consumeBatchSize);
						if (!msgs.isEmpty()) {
							ConsumerOrderlyStatus status = null;
							ConsumeMessageContext context = null;

							if (defaultMQPushConsumerImpl.hasHook()) {
								context = new ConsumeMessageContext();
								context.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
								context.setNamespace(defaultMQPushConsumer.getNamespace());
								context.setMq(messageQueue);
								context.setMsgList(msgs);
								context.setSuccess(false);
								context.setProps(new HashMap<>());

								defaultMQPushConsumerImpl.executeHookBefore(context);
							}

							long beginTimestamp = System.currentTimeMillis();
							ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
							boolean hasException = false;
							try {
								this.processQueue.getConsumeLock().readLock().lock();
								if (this.processQueue.isDropped()) {
									log.warn("consumeMessage, the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
									break;
								}

								status = messageListener.consumeMessage(Collections.unmodifiableList(msgs), new ConsumerOrderlyContext(this.messageQueue));

							}
							catch (Throwable e) {
								log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
										UtilAll.exceptionSimpleDesc(e), consumerGroup, msgs, messageQueue, e);
								hasException = true;
							}
							finally {
								this.processQueue.getConsumeLock().readLock().unlock();
							}

							if (status == null || status == ConsumerOrderlyStatus.ROLLBACK || status == ConsumerOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT) {
								log.warn("consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {}",
										consumerGroup, msgs, messageQueue);
							}

							long consumeRT = System.currentTimeMillis() - beginTimestamp;
							if (status == null) {
								returnType = hasException ? ConsumeReturnType.EXCEPTION : ConsumeReturnType.RETURNNULL;
							}
							else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
								returnType = ConsumeReturnType.TIME_OUT;
							}
							else if (status == ConsumerOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT) {
								returnType = ConsumeReturnType.FAILED;
							}
							else if (status == ConsumerOrderlyStatus.SUCCESS) {
								returnType = ConsumeReturnType.SUCCESS;
							}

							if (defaultMQPushConsumerImpl.hasHook()) {
								context.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
							}

							if (status == null) {
								status = ConsumerOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
							}

							if (defaultMQPushConsumerImpl.hasHook()) {
								context.setStatus(status.toString());
								context.setSuccess(status == ConsumerOrderlyStatus.SUCCESS || status == ConsumerOrderlyStatus.COMMIT);
								context.setAccessChannel(defaultMQPushConsumer.getAccessChannel());
								defaultMQPushConsumerImpl.executeHookAfter(context);
							}

							getConsumerStatsManager().incConsumeRT(consumerGroup, messageQueue.getTopic(), consumeRT);

							continueConsume = processConsumeResult(msgs, status, context, this);
						}
						else {
							continueConsume = false;
						}
					}
				}
				else {
					if (this.processQueue.isDropped()) {
						log.warn("run, the message queue not able to be consume, because it's dropped. {}", this.messageQueue);
						return;
					}

					tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 100);
				}
			}
		}
	}
}
