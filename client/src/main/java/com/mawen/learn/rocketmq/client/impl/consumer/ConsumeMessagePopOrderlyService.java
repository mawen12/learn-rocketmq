package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.mawen.learn.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.mawen.learn.rocketmq.client.consumer.listener.ConsumerOrderlyContext;
import com.mawen.learn.rocketmq.client.consumer.listener.ConsumerOrderlyStatus;
import com.mawen.learn.rocketmq.client.consumer.listener.MessageListenerOrderly;
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
import io.netty.util.internal.ConcurrentSet;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/25
 */
public class ConsumeMessagePopOrderlyService implements ConsumeMessageService {

	private static final Logger log = LoggerFactory.getLogger(ConsumeMessagePopOrderlyService.class);

	private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
	private final DefaultMQPushConsumer defaultMQPushConsumer;
	private final MessageListenerOrderly messageListener;
	private final BlockingQueue<Runnable> consumeRequestQueue;
	private final ConcurrentSet<ConsumeRequest> consumeRequestSet = new ConcurrentSet<>();
	private final ThreadPoolExecutor consumeExecutor;
	private final String consumerGroup;
	private final MessageQueueLock messageQueueLock = new MessageQueueLock();
	private final MessageQueueLock consumeRequestLock = new MessageQueueLock();
	private final ScheduledExecutorService scheduledExecutorService;
	private volatile boolean stopped = false;

	public ConsumeMessagePopOrderlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl, MessageListenerOrderly messageListener) {
		this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
		this.messageListener = messageListener;

		this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
		this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
		this.consumeRequestQueue = new LinkedBlockingDeque<>();

		this.consumeExecutor = new ThreadPoolExecutor(
				this.defaultMQPushConsumer.getConsumeThreadMin(),
				this.defaultMQPushConsumer.getConsumeThreadMax(),
				60 * 1000,
				TimeUnit.MILLISECONDS,
				this.consumeRequestQueue,
				new ThreadFactoryImpl("ConsumeMessageThread_"));

		this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
	}


	@Override
	public void start() {
		if (MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
			this.scheduledExecutorService.scheduleAtFixedRate(() -> {
				lockMQperiodically();
			}, 1000, ProcessQueue.REBALANCE_LOCK_INTERVAL, TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public void shutdown(long awaitTerminateMillis) {
		this.stopped = true;
		this.scheduledExecutorService.shutdown();
		ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
		if (MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
			this.unlockAllMessageQueues();
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

		final long beginTime = System.currentTimeMillis();

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
				}
			}
			else {
				result.setConsumeResult(CMResult.CR_RETURN_NULL);
			}
		}
		catch (Throwable e) {
			result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
			result.setRemark(UtilAll.exceptionSimpleDesc(e));

			log.warn("consumeMessageDirectly exception: {} Group: {} Msgs: {} MQ: {}",
					result.getRemark(), this.consumerGroup, msgs, mq, e);
		}

		result.setAutoCommit(context.isAutoCommit());
		result.setSpentTimeMillis(System.currentTimeMillis() - beginTime);

		log.info("consumeMessageDirectly Result: {}", result);

		return result;
	}

	@Override
	public void submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue, boolean dispathToConsume) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void submitPopConsumeRequest(List<MessageExt> msgs, PopProcessQueue popProcessQueue, MessageQueue messageQueue) {
		ConsumeRequest request = new ConsumeRequest(popProcessQueue, messageQueue);
		submitConsumeRequest(request, false);
	}

	public synchronized void lockMQPeriodically() {
		if (!this.stopped) {
			this.defaultMQPushConsumerImpl.getRebalanceImpl().lockAll();
		}
	}

	private void removeConsumeRequest(final ConsumeRequest request) {
		consumeRequestSet.remove(request);
	}

	public boolean processConsumeResult(final List<MessageExt> msgs, final ConsumerOrderlyStatus status, final ConsumerOrderlyContext context, final ConsumeRequest consumeRequest) {
		return true;
	}

	public ConsumerStatsManager getConsumeStatsManager() {
		return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
	}

	public boolean sendMessageBack(final MessageExt msg) {
		try {
			Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());

			MessageAccessor.setProperties(newMsg, msg.getProperties());

			String originMsgId = MessageAccessor.getOriginMessageId(msg);
			MessageAccessor.setOriginMessageId(msg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);

			newMsg.setFlag(msg.getFlag());

			MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
			MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes()));
			MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));

			newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

			this.defaultMQPushConsumerImpl.getMQClientFactory().getDefaultMQProducer().send(newMsg);
			return true;
		}
		catch (Exception e) {
			log.error("sendMessageBack exception, group: {} msg: {}", this.consumerGroup, msg, e);
		}

		return false;
	}

	public void resetNamespace(final List<MessageExt> msgs) {
		for (MessageExt msg : msgs) {
			if (StringUtils.isNotEmpty(this.defaultMQPushConsumer.getNamespace())) {
				msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
			}
		}
	}

	private void submitConsumeRequest(final ConsumeRequest request, boolean force) {
		Object lock = consumeRequestLock.fetchLockObject(request.getMessageQueue(), request.shardingKeyIndex);

		synchronized (lock) {
			boolean isNewReq = consumeRequestSet.add(request);
			if (force || isNewReq) {
				try {
					consumeExecutor.submit(request);
				}
				catch (Exception e) {
					log.error("error submit consume request: {}, mq: {}, shardingKeyIndex: {}",
							request, request.getMessageQueue(), request.getShardingKeyIndex(), e);
				}
			}
		}
	}

	private void submitConsumeRequestLater(final ConsumeRequest request, final long suspendTimeMillis) {
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

		this.scheduledExecutorService.schedule(() -> submitConsumeRequest(request, true), timeMillis, TimeUnit.MILLISECONDS);
	}

	private int getMaxReconsumeTimes() {
		if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
			return Integer.MAX_VALUE;
		}
		else {
			return this.defaultMQPushConsumer.getMaxReconsumeTimes();
		}
	}

	@Getter
	@RequiredArgsConstructor
	@AllArgsConstructor
	@EqualsAndHashCode
	class ConsumeRequest implements Runnable {
		private final PopProcessQueue processQueue;
		private final MessageQueue messageQueue;
		private int shardingKeyIndex = 0;

		@Override
		public void run() {
			if (this.processQueue.isDropped()) {
				log.warn("run, message queue not be able to consume, because it's dropped. {}", this.messageQueue);
				ConsumeMessagePopOrderlyService.class.removeConsumeRequest(this);
				return;
			}

			final Object objLock = messageQueueLock.fetchLockObject(this.messageQueue, shardingKeyIndex);
		}
	}
}
