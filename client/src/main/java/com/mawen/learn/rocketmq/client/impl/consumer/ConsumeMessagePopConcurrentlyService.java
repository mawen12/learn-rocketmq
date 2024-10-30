package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.mawen.learn.rocketmq.client.consumer.AckCallback;
import com.mawen.learn.rocketmq.client.consumer.AckResult;
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
import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.common.utils.ThreadUtils;
import com.mawen.learn.rocketmq.remoting.protocol.body.CMResult;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import com.mawen.learn.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/30
 */
public class ConsumeMessagePopConcurrentlyService implements ConsumeMessageService{

	private static final Logger log = LoggerFactory.getLogger(ConsumeMessagePopConcurrentlyService.class);

	private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
	private final DefaultMQPushConsumer defaultMQPushConsumer;
	private final MessageListenerConcurrently messageListener;
	private final BlockingQueue<Runnable> consumeRequestQueue;
	private final ThreadPoolExecutor consumeExecutor;
	private final String consumeGroup;

	private final ScheduledExecutorService scheduledExecutorService;

	public ConsumeMessagePopConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl, MessageListenerConcurrently messageListener) {
		this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
		this.messageListener = messageListener;

		this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
		this.consumeGroup = this.defaultMQPushConsumer.getConsumerGroup();
		this.consumeRequestQueue = new LinkedBlockingDeque<>();

		this.consumeExecutor = new ThreadPoolExecutor(
				this.defaultMQPushConsumer.getConsumeThreadMin(),
				this.defaultMQPushConsumer.getConsumeThreadMax(),
				60 * 1000,
				TimeUnit.MILLISECONDS,
				this.consumeRequestQueue,
				new ThreadFactoryImpl("ConsumeMessageThread_")
		);

		this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
	}

	@Override
	public void start() {
		// NOP
	}

	@Override
	public void shutdown(long awaitTerminateMillis) {
		this.scheduledExecutorService.shutdown();
		ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
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

		MessageQueue mq = new MessageQueue();
		mq.setBrokerName(brokerName);
		mq.setTopic(msg.getTopic());
		mq.setQueueId(msg.getQueueId());

		ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(mq);

		List<MessageExt> msgs = Arrays.asList(msg);

		this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumeGroup);

		final long begin = System.currentTimeMillis();

		log.info("consumeMessageDirectly receive new message: {}", msg);

		try {
			ConsumeConcurrentlyStatus status = this.messageListener.consumeMessage(msgs, context);
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

			log.warn("consumeMessageDirectly exception: {}, group: {}, Msgs: {}, MQ: {}",
					result.getRemark(), this.consumeGroup, msg, mq, e);
		}

		result.setSpentTimeMillis(System.currentTimeMillis() - begin);

		log.info("consumeMessageDirectly Result: {}", result);

		return result;
	}

	@Override
	public void submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue, boolean dispathToConsume) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void submitPopConsumeRequest(List<MessageExt> msgs, PopProcessQueue popProcessQueue, MessageQueue messageQueue) {
		int consumeMessageBatchMaxSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
		if (msgs.size() < consumeMessageBatchMaxSize) {
			ConsumeRequest consumeRequest = new ConsumeRequest(msgs, popProcessQueue, messageQueue);
			try {
				this.consumeExecutor.submit(consumeRequest);
			}
			catch (RejectedExecutionException e) {
				this.submitConsumeRequestLater(consumeRequest);
			}
		}
		else {
			for (int total = 0; total < msgs.size();) {
				List<MessageExt> msgThis = new ArrayList<>(consumeMessageBatchMaxSize);
				for (int i = 0; i < consumeMessageBatchMaxSize; i++, total++) {
					if (total < msgs.size()) {
						msgThis.add(msgs.get(total));
					}
					else {
						break;
					}
				}

				ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, popProcessQueue, messageQueue);
				try {
					this.consumeExecutor.submit(consumeRequest);
				}
				catch (RejectedExecutionException e) {
					for (; total < msgs.size(); total++) {
						msgThis.add(msgs.get(total));
					}

					this.submitConsumeRequestLater(consumeRequest);
				}
			}
		}
	}

	public void processConsumeResult(final ConsumeConcurrentlyStatus status, final ConsumeConcurrentlyContext context, final ConsumeRequest consumeRequest) {
		if (consumeRequest.getMsgs().isEmpty()) {
			return;
		}

		int ackIndex = context.getAckIndex();
		String topic = consumeRequest.getMessageQueue().getTopic();

		switch (status) {
			case CONSUME_SUCCESS:
				if (ackIndex >= consumeRequest.getMsgs().size()) {
					ackIndex = consumeRequest.getMsgs().size() - 1;
				}
				int ok = ackIndex + 1;
				int failed = consumeRequest.getMsgs().size() - ok;
				this.getConsumerStatsManager().incConsumeOKTPS(consumeGroup, topic, ok);
				this.getConsumerStatsManager().incConsumeFailedTPS(consumeGroup, topic, failed);
				break;
			case RECONSUME_LATER:
				ackIndex = -1;
				this.getConsumerStatsManager().incConsumeFailedTPS(consumeGroup, topic, consumeRequest.getMsgs().size());
				break;
			default:
				break;
		}

		for (int i = 0; i < ackIndex; i++) {
			this.defaultMQPushConsumerImpl.ackAsync(consumeRequest.getMsgs().get(i), consumeGroup);
			consumeRequest.getProcessQueue().ack();
		}

		for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
			MessageExt messageExt = consumeRequest.getMsgs().get(i);
			consumeRequest.getProcessQueue().ack();
			if (messageExt.getReconsumeTimes() >= this.defaultMQPushConsumerImpl.getMaxReconsumeTimes()) {
				checkNeedAckOrDelay(messageExt);
				continue;
			}

			int delayLevel = context.getDelayLevelWhenNextConsume();
			changePopInvisibleTime(consumeRequest.getMsgs().get(i), consumeGroup, delayLevel);
		}
	}

	public ConsumerStatsManager getConsumerStatsManager() {
		return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
	}

	private void checkNeedAckOrDelay(MessageExt ext) {
		int[] delayLevelTable = this.defaultMQPushConsumerImpl.getPopDelayLevel();
		long msgDelayTime = System.currentTimeMillis() - ext.getBornTimestamp();

		if (msgDelayTime > delayLevelTable[delayLevelTable.length - 1] * 1000 * 2) {
			log.warn("Consume too many times, ack message async. message {}", ext);
			this.defaultMQPushConsumerImpl.ackAsync(ext, consumeGroup);
		}
		else {
			int delayLevel = delayLevelTable.length - 1;
			for (; delayLevel >= 0; delayLevel--) {
				if (msgDelayTime >= delayLevelTable[delayLevel] * 1000) {
					delayLevel++;
					break;
				}
			}

			changePopInvisibleTime(ext, consumeGroup, delayLevel);
			log.warn("Consume too many times, but delay time {} not enough. changePopInvisibleTime to delayLevel {}. message key: {}",
					msgDelayTime, delayLevel, ext.getKeys());
		}
	}

	private void changePopInvisibleTime(final MessageExt msg, String consumeGroup, int delayLevel) {
		if (delayLevel == 0) {
			delayLevel = msg.getReconsumeTimes();
		}

		int[] delayLevelTable = this.defaultMQPushConsumerImpl.getPopDelayLevel();
		int delaySecond = delayLevel >= delayLevelTable.length ? delayLevelTable[delayLevelTable.length - 1] : delayLevelTable[delayLevel];
		String extraInfo = msg.getProperty(MessageConst.PROPERTY_POP_CK);

		try {
			this.defaultMQPushConsumerImpl.changePopInvisibleTimeAsync(msg.getTopic(), consumeGroup, extraInfo, delaySecond * 1000, new AckCallback() {
				@Override
				public void onSuccess(AckResult ackResult) {
				}

				@Override
				public void onException(Throwable e) {
					log.error("changePopInvisibleTimeAsync fail. msg: {} error info: {}", msg, e);
				}
			});
		}
		catch (Throwable e) {
			log.error("changePopInvisibleTime fail, group: {} msg: {} errorInfo: {}",
					consumeGroup, msg, e);
		}
	}

	private void submitConsumeRequestLater(final ConsumeRequest consumeRequest) {
		this.scheduledExecutorService.schedule(() -> {
			ConsumeMessagePopConcurrentlyService.this.consumeExecutor.submit(consumeRequest);
		}, 5000, TimeUnit.MILLISECONDS);
	}

	@Getter
	class ConsumeRequest implements Runnable {
		private final List<MessageExt> msgs;
		private final PopProcessQueue processQueue;
		private final MessageQueue messageQueue;
		private long popTime = 0;
		private long invisibleTime = 0;

		public ConsumeRequest(List<MessageExt> msgs, PopProcessQueue processQueue, MessageQueue messageQueue) {
			this.msgs = msgs;
			this.processQueue = processQueue;
			this.messageQueue = messageQueue;

			try {
				String extraInfo = msgs.get(0).getProperty(MessageConst.PROPERTY_POP_CK);
				String[] extraInfoStrs = ExtraInfoUtil.split(extraInfo);
				popTime = ExtraInfoUtil.getPopTime(extraInfoStrs);
				invisibleTime = ExtraInfoUtil.getInvisibleTime(extraInfoStrs);
			}
			catch (Throwable t) {
				log.error("parse extra info error. msg: {}", msgs.get(0), t);
			}
		}

		public boolean isPopTimeout() {
			if (msgs.size() == 0 || popTime <= 0 || invisibleTime <= 0) {
				return true;
			}

			long current = System.currentTimeMillis();
			return current - popTime >= invisibleTime;
		}

		@Override
		public void run() {
			if (this.processQueue.isDropped()) {
				log.info("the message queue not be able to consume, because it's dropped(pop). group={} {}",
						ConsumeMessagePopConcurrentlyService.this.consumeGroup, this.messageQueue);
				return;
			}

			if (isPopTimeout()) {
				log.info("the pop message timeout so abort consume. popTime={}, invisibleTime={}, group={} {}",
						popTime, invisibleTime, ConsumeMessagePopConcurrentlyService.this.consumeGroup, this.messageQueue);
				processQueue.decFoundMsg(-msgs.size());
				return;
			}

			MessageListenerConcurrently listener = ConsumeMessagePopConcurrentlyService.this.messageListener;
			ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
			ConsumeConcurrentlyStatus status = null;
			defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

			ConsumeMessageContext consumeMessageContext = null;
			if (ConsumeMessagePopConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
				consumeMessageContext = new ConsumeMessageContext();
				consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
				consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
				consumeMessageContext.setProps(new HashMap<>());
				consumeMessageContext.setMq(this.messageQueue);
				consumeMessageContext.setMsgList(this.msgs);
				consumeMessageContext.setSuccess(false);

				ConsumeMessagePopConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
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
				status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);
			}
			catch (Throwable e) {
				log.warn("consumeMessage exception: {} Group: {} Msg: {} MQ: {}",
						UtilAll.exceptionSimpleDesc(e),  ConsumeMessagePopConcurrentlyService.this.consumeGroup, msgs, messageQueue);
				hasException = true;
			}

			long consumeRT = System.currentTimeMillis() - begin;
			if (status == null) {
				returnType = hasException ? ConsumeReturnType.EXCEPTION : ConsumeReturnType.RETURNNULL;
			}
			else if (consumeRT >= invisibleTime * 1000) {
				returnType = ConsumeReturnType.TIME_OUT;
			}
			else if (status == ConsumeConcurrentlyStatus.RECONSUME_LATER) {
				returnType = ConsumeReturnType.FAILED;
			}
			else if (status == ConsumeConcurrentlyStatus.CONSUME_SUCCESS) {
				returnType = ConsumeReturnType.SUCCESS;
			}

			if (status == null) {
				log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",
						ConsumeMessagePopConcurrentlyService.this.consumeGroup, msgs, messageQueue);
				status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
			}

			if (ConsumeMessagePopConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
				consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
				consumeMessageContext.setStatus(status.toString());
				consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
				consumeMessageContext.setAccessChannel(defaultMQPushConsumer.getAccessChannel());

				ConsumeMessagePopConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
			}

			ConsumeMessagePopConcurrentlyService.this.getConsumerStatsManager().incConsumeRT(ConsumeMessagePopConcurrentlyService.this.consumeGroup, messageQueue.getTopic(), consumeRT);

			if (!processQueue.isDropped() && !isPopTimeout()) {
				ConsumeMessagePopConcurrentlyService.this.processConsumeResult(status, context, this);
			}
			else {
				if (msgs != null) {
					processQueue.decFoundMsg(-msgs.size());
				}

				log.warn("processQueue invalid. idDropped={}, isPopTimeout={}, messageQueue={}, msgs={}",
						processQueue.isDropped(), isPopTimeout(), messageQueue, msgs);
			}
		}
	}
}
