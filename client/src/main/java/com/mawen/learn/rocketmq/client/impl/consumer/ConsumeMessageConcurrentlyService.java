package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.mawen.learn.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.mawen.learn.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.mawen.learn.rocketmq.common.ThreadFactoryImpl;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.common.utils.ThreadUtils;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/25
 */
public class ConsumeMessageConcurrentlyService implements ConsumeMessageService{

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
		return null;
	}

	@Override
	public void submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue, boolean dispathToConsume) {

	}

	@Override
	public void submitPopConsumeRequest(List<MessageExt> msgs, PopProcessQueue popProcessQueue, MessageQueue messageQueue) {

	}
}
