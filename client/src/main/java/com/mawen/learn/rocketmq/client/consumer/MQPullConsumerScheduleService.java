package com.mawen.learn.rocketmq.client.consumer;

import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.mawen.learn.rocketmq.common.ThreadFactoryImpl;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.protocol.NamespaceUtil;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.MessageModel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
@Getter
@Setter
public class MQPullConsumerScheduleService {

	private static final Logger log = LoggerFactory.getLogger(MQPullConsumerScheduleService.class);

	private final MessageQueueListener messageQueueListener = new MessageQueueListenerImpl();
	private final ConcurrentMap<MessageQueue, PullTaskImpl> taskTable = new ConcurrentHashMap<>();
	private DefaultMQPullConsumer defaultMQPullConsumer;
	private int pullThreadNums = 20;
	private ConcurrentMap<String, PullTaskCallback> callbackTable = new ConcurrentHashMap<>();
	private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

	public MQPullConsumerScheduleService(String consumerGroup) {
		this.defaultMQPullConsumer = new DefaultMQPullConsumer(consumerGroup);
		this.defaultMQPullConsumer.setMessageModel(MessageModel.CLUSTERING);
	}

	public MQPullConsumerScheduleService(String consumerGroup, final RPCHook rpcHook) {
		this.defaultMQPullConsumer = new DefaultMQPullConsumer(consumerGroup, rpcHook);
		this.defaultMQPullConsumer.setMessageModel(MessageModel.CLUSTERING);
	}

	public void start() {
		String consumerGroup = this.defaultMQPullConsumer.getConsumerGroup();

		this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(this.pullThreadNums, new ThreadFactoryImpl("PullMsgThread-" + consumerGroup));

		this.defaultMQPullConsumer.setMessageQueueListener(this.messageQueueListener);

		this.defaultMQPullConsumer.start();

		log.info("MQPullConsumerScheduleService start OK, {} {}", consumerGroup, this.callbackTable);
	}

	public void shutdown() {
		if (this.scheduledThreadPoolExecutor != null) {
			this.scheduledThreadPoolExecutor.shutdown();
		}
		if (this.defaultMQPullConsumer != null) {
			this.defaultMQPullConsumer.shutdown();
		}
	}

	public void putTask(String topic, Set<MessageQueue> mqNewSet) {
		Iterator<Map.Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<MessageQueue, PullTaskImpl> next = it.next();
			if (next.getKey().getTopic().equals(topic)) {
				if (!mqNewSet.contains(next.getKey())) {
					next.getValue().setCancelled(true);
					it.remove();
				}
			}
		}

		for (MessageQueue mq : mqNewSet) {
			if (!this.taskTable.containsKey(mq)) {
				PullTaskImpl command = new PullTaskImpl(mq);
				this.taskTable.put(mq, command);
				this.scheduledThreadPoolExecutor.schedule(command, 0, TimeUnit.MILLISECONDS);
			}
		}
	}

	public void registerPullTaskCallback(final String topic, final PullTaskCallback callback) {
		this.callbackTable.put(NamespaceUtil.wrapNamespace(this.defaultMQPullConsumer.getNamespace(), topic), callback);
		this.defaultMQPullConsumer.registerMessageQueueListener(topic, null);
	}

	class MessageQueueListenerImpl implements MessageQueueListener {

		@Override
		public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqAssigned) {
			MessageModel messageModel = MQPullConsumerScheduleService.this.defaultMQPullConsumer.getMessageModel();
			switch (messageModel) {
				case BROADCASTING:
					MQPullConsumerScheduleService.this.putTask(topic, mqAll);
					break;
				case CLUSTERING:
					MQPullConsumerScheduleService.this.putTask(topic, mqAssigned);
					break;
				default:
					break;
			}
		}
	}

	@Getter
	@Setter
	@RequiredArgsConstructor
	public class PullTaskImpl implements Runnable {
		private final MessageQueue messageQueue;
		private volatile boolean cancelled = false;

		@Override
		public void run() {
			String topic = this.messageQueue.getTopic();

			if (!this.isCancelled()) {
				PullTaskCallback callback = MQPullConsumerScheduleService.this.callbackTable.get(topic);

				if (callback != null) {
					PullTaskContext context = new PullTaskContext();
					context.setPullConsumer(MQPullConsumerScheduleService.this.defaultMQPullConsumer);

					try {
						callback.doPullTask(this.messageQueue, context);
					}
					catch (Throwable e) {
						context.setPullNextDelayTimeMillis(1000);
						log.error("doPullTask Exception", e);
					}

					if (!this.isCancelled()) {
						MQPullConsumerScheduleService.this.scheduledThreadPoolExecutor.schedule(this, context.getPullNextDelayTimeMillis(), TimeUnit.MILLISECONDS);
					}
					else {
						log.warn("The Pull Task is cancelled after doPullTask, {}", messageQueue);
					}
				}
				else {
					log.warn("Pull Task Callback not exist, {}", topic);
				}
			}
			else {
				log.warn("The Pull Task is cancelled, {}", messageQueue);
			}
		}
	}
}
