package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import com.mawen.learn.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.mawen.learn.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
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

	@Override
	public void start() {

	}

	@Override
	public void shutdown(long awaitTerminateMillis) {

	}

	@Override
	public void incCorePoolSize() {

	}

	@Override
	public void decCorePoolSize() {

	}

	@Override
	public int getCorePoolSize() {
		return 0;
	}

	@Override
	public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
		return null;
	}

	@Override
	public void submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue, boolean dispathToConsume) {

	}

	@Override
	public void submitConsumeRequest(List<MessageExt> msgs, PopProcessQueue popProcessQueue, MessageQueue messageQueue) {

	}
}
