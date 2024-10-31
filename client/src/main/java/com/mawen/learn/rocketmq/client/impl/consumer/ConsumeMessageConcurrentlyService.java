package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.mawen.learn.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.mawen.learn.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.mawen.learn.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.mawen.learn.rocketmq.client.consumer.listener.ConsumeReturnType;
import com.mawen.learn.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.mawen.learn.rocketmq.client.hook.ConsumeMessageContext;
import com.mawen.learn.rocketmq.client.stat.ConsumerStatsManager;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.ThreadFactoryImpl;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.message.MessageAccessor;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.common.utils.ThreadUtils;
import com.mawen.learn.rocketmq.remoting.protocol.body.CMResult;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/25
 */
public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {

	private static final Logger log = LoggerFactory.getLogger(ConsumeMessageConcurrentlyService.class);

	private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
	private final DefaultMQPushConsumer defaultMQPushConsumer;
	private final MessageListenerConcurrently messageListener;
	private final BlockingQueue<Runnable> consumeRequestQueue;
	private final ThreadPoolExecutor consumeExecutor;
	private final String consumerGroup;

	private final ScheduledExecutorService scheduledExecutorService;
	private final ScheduledExecutorService cleanExpireMsgExecutors;

	public ConsumeMessageConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl, MessageListenerConcurrently messageListener) {
		this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
		this.messageListener = messageListener;

		this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
		this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
		this.consumeRequestQueue = new LinkedBlockingQueue<>();

		String consumeGroupTag = consumerGroup.length() > 100 ? consumerGroup.substring(0, 100) : consumerGroup;
		this.consumeExecutor = new ThreadPoolExecutor(
				this.defaultMQPushConsumer.getConsumeThreadMin(),
				this.defaultMQPushConsumer.getConsumeThreadMax(),
				60 * 1000,
				TimeUnit.MILLISECONDS,
				this.consumeRequestQueue,
				new ThreadFactoryImpl("ConsumeMessageThread_" + consumeGroupTag)
		);

		this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_" + consumeGroupTag));
		this.cleanExpireMsgExecutors = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CleanExpireMsgScheduledThread_" + consumeGroupTag));
	}

	@Override
	public void start() {
		this.cleanExpireMsgExecutors.scheduleAtFixedRate(() -> {
			try {
				cleanExpireMsg();
			}
			catch (Throwable e) {
				log.error("scheduleAtFixedRate cleanExpireMsg exception", e);
			}
		}, this.defaultMQPushConsumer.getConsumeTimeout(), this.defaultMQPushConsumer.getConsumeTimeout(), TimeUnit.MINUTES);
	}

	@Override
	public void shutdown(long awaitTerminateMillis) {
		this.scheduledExecutorService.shutdown();
		ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
		this.cleanExpireMsgExecutors.shutdown();
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
		result.setOrder(false);
		result.setAutoCommit(true);

		msg.setBrokerName(brokerName);
		List<MessageExt> msgs = new ArrayList<>();
		msgs.add(msg);

		MessageQueue mq = new MessageQueue();
		mq.setBrokerName(brokerName);
		mq.setTopic(msg.getTopic());
		mq.setQueueId(msg.getQueueId());

		ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(mq);

		defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, consumerGroup);

		long begin = System.currentTimeMillis();

		log.info("consumeMessageDirectly receive new message: {}", msg);

		try {
			ConsumeConcurrentlyStatus status = messageListener.consumeMessage(msgs, context);
			if (status != null) {
				switch (status) {
					case CONSUME_SUCCESS:
						result.setConsumeResult(CMResult.CR_SUCCESS);
						break;
					case RECONSUME_LATER:
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

			log.warn("consumeMessageDirectly exception: {} Group: {} Msg: {} MQ: {}",
					result.getRemark(), consumerGroup, msg, mq, e);
		}

		result.setSpentTimeMillis(System.currentTimeMillis() - begin);

		log.info("consumeMessageDirectly Result: {}", result);

		return result;
	}

	@Override
	public void submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue, boolean dispathToConsume) {
		int consumeBatchSize = defaultMQPushConsumer.getConsumeMessageBatchMaxSize();

		if (msgs.size() <= consumeBatchSize) {
			ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
			try {
				consumeExecutor.submit(consumeRequest);
			}
			catch (RejectedExecutionException e) {
				submitConsumeRequestLater(consumeRequest);
			}
		}
		else {
			for (int total = 0; total < msgs.size();) {
				List<MessageExt> msgThis = new ArrayList<>(consumeBatchSize);
				for (int i = 0; i < consumeBatchSize; i++, total++) {
					if (total < msgs.size()) {
						msgThis.add(msgs.get(total));
					}
					else {
						break;
					}
				}

				ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
				try {
					consumeExecutor.submit(consumeRequest);
				}
				catch (RejectedExecutionException e) {
					for (; total < msgs.size(); total++) {
						msgs.add(msgs.get(total));
					}

					submitConsumeRequestLater(consumeRequest);
				}
			}
		}
	}

	@Override
	public void submitPopConsumeRequest(List<MessageExt> msgs, PopProcessQueue popProcessQueue, MessageQueue messageQueue) {
		throw new UnsupportedOperationException();
	}

	public void processConsumeResult(final ConsumeConcurrentlyStatus status, final ConsumeConcurrentlyContext context, final ConsumeRequest consumeRequest) {
		int ackIndex = context.getAckIndex();
		if (consumeRequest.getMsgs().isEmpty()) {
			return;
		}

		int msgSize = consumeRequest.getMsgs().size();

		switch (status) {
			case CONSUME_SUCCESS:
				if (ackIndex >= msgSize) {
					ackIndex = msgSize - 1;
				}
				int ok = ackIndex = 1;
				int failed = msgSize - ok;

				getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), ok);
				getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), failed);
				break;
			case RECONSUME_LATER:
				ackIndex = -1;
				getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgSize);
				break;
			default:
				break;
		}

		switch (defaultMQPushConsumer.getMessageModel()) {
			case BROADCASTING:
				for (int i = ackIndex + 1; i < msgSize; i++) {
					MessageExt msg = consumeRequest.getMsgs().get(i);
					log.warn("BROADCASTING, the message consume failed, drop it, {}", msg);
				}
				break;
			case CLUSTERING:
				List<MessageExt> msgBackFailed = new ArrayList<>(msgSize);
				for (int i = ackIndex + 1; i < msgSize; i++) {
					MessageExt msg = consumeRequest.getMsgs().get(i);
					if (!consumeRequest.getProcessQueue().containsMessage(msg)) {
						log.info("Message is not found in its process queue; skip send-back-procedure, topic={}, brokerName={}, queueId={}, queueOffset={}",
								msg.getTopic(), msg.getBrokerName(), msg.getQueueId(), msg.getQueueOffset());
						continue;
					}
					boolean result = sendMessageBack(msg, context);
					if (!result) {
						msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
						msgBackFailed.add(msg);
					}
				}

				if (!msgBackFailed.isEmpty()) {
					consumeRequest.getMsgs().removeAll(msgBackFailed);
					submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue());
				}
				break;
			default:
				break;
		}

		long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());
		if (offset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
			defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), offset, true);
		}
	}

	public ConsumerStatsManager getConsumerStatsManager() {
		return defaultMQPushConsumerImpl.getConsumerStatsManager();
	}

	public boolean sendMessageBack(final MessageExt msg, final ConsumeConcurrentlyContext context) {
		int delayLevel = context.getDelayLevelWhenNextConsume();
		msg.setTopic(defaultMQPushConsumer.withNamespace(msg.getTopic()));

		try {
			MessageQueue mq = defaultMQPushConsumer.queueWithNamespace(context.getMessageQueue());
			defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, mq);
			return true;
		}
		catch (Exception e) {
			log.warn("sendMessageBack exception, group: {} msg: {}", consumerGroup, msg, e);
		}
		return false;
	}

	private void cleanExpireMsg() {
		Iterator<Map.Entry<MessageQueue, ProcessQueue>> iterator = defaultMQPushConsumerImpl.getRebalanceImpl().getProcessQueueTable().entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<MessageQueue, ProcessQueue> next = iterator.next();
			ProcessQueue pq = next.getValue();
			pq.cleanExpiredMsg(defaultMQPushConsumer);
		}
	}

	private void submitConsumeRequestLater(final List<MessageExt> msgs, final ProcessQueue processQueue, final MessageQueue messageQueue) {
		scheduledExecutorService.schedule(() -> submitConsumeRequest(msgs, processQueue, messageQueue, true), 5000, TimeUnit.MILLISECONDS);
	}

	private void submitConsumeRequestLater(final ConsumeRequest consumeRequest) {
		scheduledExecutorService.schedule(() -> consumeExecutor.submit(consumeRequest), 5000, TimeUnit.MILLISECONDS);
	}

	@Getter
	@RequiredArgsConstructor
	class ConsumeRequest implements Runnable {
		private final List<MessageExt> msgs;
		private final ProcessQueue processQueue;
		private final MessageQueue messageQueue;

		@Override
		public void run() {
			if (processQueue.isDropped()) {
				log.info("the message queue not be able to consume, because it's dropped, group={} {}", consumerGroup, messageQueue);
				return;
			}

			ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
			defaultMQPushConsumerImpl.tryResetPopRetryTopic(msgs, consumerGroup);
			defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

			ConsumeConcurrentlyStatus status = null;
			ConsumeMessageContext consumeMessageContext = null;
			if (defaultMQPushConsumerImpl.hasHook()) {
				consumeMessageContext = new ConsumeMessageContext();
				consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
				consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
				consumeMessageContext.setProps(new HashMap<>());
				consumeMessageContext.setMq(messageQueue);
				consumeMessageContext.setMsgList(msgs);
				consumeMessageContext.setSuccess(false);
				defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
			}

			long begin = System.currentTimeMillis();
			boolean hasException = false;
			ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
			try {
				if (CollectionUtils.isNotEmpty(msgs)) {
					for (MessageExt msg : msgs) {
						MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
					}
				}
				status = messageListener.consumeMessage(Collections.unmodifiableList(msgs), context);
			}
			catch (Throwable e) {
				log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
						UtilAll.exceptionSimpleDesc(e), consumerGroup, msgs, messageQueue, e);
				hasException = true;
			}

			long consumeRT = System.currentTimeMillis() - begin;
			if (status == null) {
				returnType = hasException ? ConsumeReturnType.EXCEPTION : ConsumeReturnType.RETURNNULL;
			}
			else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
				returnType = ConsumeReturnType.TIME_OUT;
			}
			else if (status == ConsumeConcurrentlyStatus.RECONSUME_LATER) {
				returnType = ConsumeReturnType.FAILED;
			}
			else if (status == ConsumeConcurrentlyStatus.CONSUME_SUCCESS) {
				returnType = ConsumeReturnType.SUCCESS;
			}

			if (defaultMQPushConsumerImpl.hasHook()) {
				consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
			}

			if (status == null) {
				log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",
						consumerGroup, msgs, messageQueue);
				status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
			}

			if (defaultMQPushConsumerImpl.hasHook()) {
				consumeMessageContext.setStatus(status.toString());
				consumeMessageContext.setSuccess(status == ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
				consumeMessageContext.setAccessChannel(defaultMQPushConsumer.getAccessChannel());

				defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
			}

			getConsumerStatsManager().incConsumeRT(consumerGroup, messageQueue.getTopic(), consumeRT);

			if (!processQueue.isDropped()) {
				processConsumeResult(status, context, this);
			}
			else {
				log.warn("processQueue is dropped without process consume result, messageQueue={} msgs={}", messageQueue, msgs);
			}
		}
	}
}
