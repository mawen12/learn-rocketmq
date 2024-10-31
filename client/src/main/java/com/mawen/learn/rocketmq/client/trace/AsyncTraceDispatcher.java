package com.mawen.learn.rocketmq.client.trace;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.mawen.learn.rocketmq.client.AccessChannel;
import com.mawen.learn.rocketmq.client.common.ThreadLocalIndex;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import com.mawen.learn.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import com.mawen.learn.rocketmq.client.impl.producer.TopicPublishInfo;
import com.mawen.learn.rocketmq.client.producer.DefaultMQProducer;
import com.mawen.learn.rocketmq.client.producer.MessageQueueSelector;
import com.mawen.learn.rocketmq.client.producer.SendCallback;
import com.mawen.learn.rocketmq.client.producer.SendResult;
import com.mawen.learn.rocketmq.common.ThreadFactoryImpl;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.common.topic.TopicValidator;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/17
 */
public class AsyncTraceDispatcher implements TraceDispatcher {

	private static final Logger log = LoggerFactory.getLogger(AsyncTraceDispatcher.class);

	private static final AtomicInteger COUNTER = new AtomicInteger();
	private static final short MAX_MSG_KEY_SIZE = Short.MAX_VALUE - 10000;

	private final int queueSize;
	private final int batchSize;
	private final int maxMsgSize;
	private final long pollingTimeMil;
	private final long waitTimeThresholdMil;
	@Getter @Setter
	private final DefaultMQProducer traceProducer;
	private final ThreadPoolExecutor traceExecutor;
	private AtomicLong discardCount;
	private Thread worker;
	private final ArrayBlockingQueue<TraceContext> traceContextQueue;
	private final HashMap<String, TraceDataSegment> taskQueueByTopic;
	private ArrayBlockingQueue<Runnable> appenderQueue;
	private volatile Thread shutdownHook;
	private volatile boolean stopped = false;
	@Getter @Setter
	private DefaultMQProducerImpl hostProducer;
	@Getter @Setter
	private DefaultMQPushConsumerImpl hostConsumer;
	private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
	private String dispatcherId = UUID.randomUUID().toString();
	@Getter @Setter
	private volatile String traceTopicName;
	private AtomicBoolean isStart = new AtomicBoolean(false);
	@Getter @Setter
	private volatile AccessChannel accessChannel = AccessChannel.LOCAL;
	private String group;
	private Type type;
	@Getter @Setter
	private String namespaceV2;

	public AsyncTraceDispatcher(String group, Type type, String traceTopicName, RPCHook rpcHook) {
		this.queueSize = 2048;
		this.batchSize = 100;
		this.maxMsgSize = 128000;
		this.pollingTimeMil = 100;
		this.waitTimeThresholdMil = 500;
		this.discardCount = new AtomicLong(0L);
		this.traceContextQueue = new ArrayBlockingQueue<>(1024);
		this.taskQueueByTopic = new HashMap<>();
		this.group = group;
		this.type = type;

		this.appenderQueue = new ArrayBlockingQueue<>(queueSize);
		if (!UtilAll.isBlank(traceTopicName)) {
			this.traceTopicName = traceTopicName;
		}
		else {
			this.traceTopicName = TopicValidator.RMQ_SYS_TRACE_TOPIC;
		}

		this.traceExecutor = new ThreadPoolExecutor(10, 20, 60 * 1000, TimeUnit.MILLISECONDS, this.appenderQueue, new ThreadFactoryImpl("MQTraceSendThread_"));
		this.traceProducer = getAndCreateTraceProducer(rpcHook);
	}

	@Override
	public void start(String namesrvAddr, AccessChannel accessChannel) throws MQClientException {
		if (isStart.compareAndSet(false, true)) {
			traceProducer.setNamesrvAddr(namesrvAddr);
			traceProducer.setInstanceName(TraceConstants.TRACE_INSTANCE_NAME + "-" + namesrvAddr);
			traceProducer.setNamespaceV2(namespaceV2);
			traceProducer.setEnableTrace(false);
			traceProducer.start();
		}

		this.accessChannel = accessChannel;
		this.worker = new Thread(new AsyncRunnable(), "MQ-AsyncTraceDispatcher-Thread-" + dispatcherId);
		this.worker.setDaemon(true);
		this.worker.start();

		this.registerShutdownHook();
	}

	@Override
	public boolean append(Object obj) {
		boolean result = traceContextQueue.offer((TraceContext) obj);
		if (!result) {
			log.info("buffer full {}, context is {}", discardCount.incrementAndGet(), obj);
		}
		return result;
	}

	@Override
	public void flush() {
		long end = System.currentTimeMillis() + 500;
		while (System.currentTimeMillis() <= end) {
			synchronized (this.taskQueueByTopic) {
				for (TraceDataSegment taskInfo : taskQueueByTopic.values()) {
					taskInfo.sendAllData();
				}

				synchronized (this.traceContextQueue) {
					if (traceContextQueue.size() == 0 && appenderQueue.size() == 0) {
						break;
					}
				}

				try {
					Thread.sleep(1);
				}
				catch (InterruptedException ignored) {
					break;
				}
			}
		}
		log.info("------end trace send {}   {}", traceContextQueue.size(), appenderQueue.size());
	}

	@Override
	public void shutdown() {
		this.stopped = true;

		flush();

		this.traceExecutor.shutdown();

		if (isStart.get()) {
			traceProducer.shutdown();
		}

		this.removeShutdownHook();
	}

	public void registerShutdownHook() {
		if (shutdownHook == null) {
			shutdownHook = new Thread(new Runnable() {
				private volatile boolean hasShutdown = false;

				@Override
				public void run() {
					synchronized (this) {
						if (!this.hasShutdown) {
							flush();
						}
					}
				}
			}, "ShutdownHookMQTrace");

			Runtime.getRuntime().addShutdownHook(shutdownHook);
		}
	}

	public void removeShutdownHook() {
		if (shutdownHook != null) {
			try {
				Runtime.getRuntime().removeShutdownHook(shutdownHook);
			}
			catch (IllegalStateException ignored) {}
		}
	}

	private DefaultMQProducer getAndCreateTraceProducer(RPCHook rpcHook) {
		DefaultMQProducer traceProducerInstance = this.traceProducer;
		if (traceProducerInstance == null) {
			traceProducerInstance = new DefaultMQProducer(rpcHook);
			traceProducerInstance.setProducerGroup(genGroupNameForTrace());
			traceProducerInstance.setSendMsgTimeout(5000);
			traceProducerInstance.setVipChannelEnabled(false);
			traceProducerInstance.setMaxMessageSize(maxMsgSize);
		}
		return traceProducerInstance;
	}

	private String genGroupNameForTrace() {
		return TraceConstants.GROUP_NAME_PREFIX + "-" + this.group + "-" + this.type + "-" + COUNTER.incrementAndGet();
	}

	class AsyncRunnable implements Runnable {
		private boolean stopped;

		@Override
		public void run() {
			while (!stopped) {
				synchronized (traceContextQueue) {

					long endTime = System.currentTimeMillis() + pollingTimeMil;
					while (System.currentTimeMillis() < endTime) {

						try {
							TraceContext traceContext = traceContextQueue.poll(endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);

							if (traceContext != null && !traceContext.getTraceBeans().isEmpty()) {
								String traceTopicName = this.getTraceTopicName(traceContext.getRegionId());
								TraceDataSegment traceDataSegment = taskQueueByTopic.get(traceTopicName);
								if (traceDataSegment == null) {
									traceDataSegment = new TraceDataSegment(traceTopicName, traceContext.getRegionId());
									taskQueueByTopic.put(traceTopicName, traceDataSegment);
								}

								TraceTransferBean bean = TraceDataEncoder.encodeFromContextBean(traceContext);
								traceDataSegment.addTraceTransferBean(bean);
							}
						}
						catch (InterruptedException ignored) {
							log.error("traceContextQueue#poll exception");
						}

						sendDataByTimeThreshold();

						if (AsyncTraceDispatcher.this.stopped) {
							this.stopped = true;
						}
					}
				}
			}
		}

		private void sendDataByTimeThreshold() {
			long now = System.currentTimeMillis();
			for (TraceDataSegment taskInfo : taskQueueByTopic.values()) {
				if (now - taskInfo.firstBeanAddTime >= waitTimeThresholdMil) {
					taskInfo.sendAllData();
				}
			}
		}

		private String getTraceTopicName(String regionId) {
			AccessChannel channel = AsyncTraceDispatcher.this.getAccessChannel();
			if (channel == AccessChannel.CLOUD) {
				return TraceConstants.TRACE_TOPIC_PREFIX + regionId;
			}
			return AsyncTraceDispatcher.this.getTraceTopicName();
		}
	}

	@RequiredArgsConstructor
	class TraceDataSegment {
		private long firstBeanAddTime;
		private int currentMsgSize;
		private int currentMsgKeySize;
		private final String traceTopicTime;
		private final String regionId;
		private final List<TraceTransferBean> traceTransferBeanList = new ArrayList<>();

		public void addTraceTransferBean(TraceTransferBean bean) {
			initFirstBeanAddTime();

			this.traceTransferBeanList.add(bean);
			this.currentMsgSize += bean.getTransData().length();
			this.currentMsgKeySize = bean.getTransKey().stream().reduce(currentMsgKeySize, (acc, x) -> acc + x.length(), Integer::sum);

			if (currentMsgSize >= traceProducer.getMaxMessageSize() - 10 * 1000 || currentMsgKeySize >= MAX_MSG_KEY_SIZE) {
				this.doSend();
			}
		}

		public void sendAllData() {
			if (this.traceTransferBeanList.isEmpty()) {
				return;
			}

			this.doSend();
		}

		private void doSend() {
			List<TraceTransferBean> dataToSend = new ArrayList<>(traceTransferBeanList);
			AsyncDataSendTask task = new AsyncDataSendTask(traceTopicName, regionId, dataToSend);
			traceExecutor.submit(task);
			this.clear();
		}

		private void initFirstBeanAddTime() {
			if (firstBeanAddTime == 0) {
				firstBeanAddTime = System.currentTimeMillis();
			}
		}

		private void clear() {
			this.firstBeanAddTime = 0;
			this.currentMsgSize = 0;
			this.currentMsgKeySize = 0;
			this.traceTransferBeanList.clear();
		}
	}

	@AllArgsConstructor
	class AsyncDataSendTask implements Runnable {

		private final String traceTopicName;
		private final String regionId;
		private final List<TraceTransferBean> traceTransferBeanList;

		@Override
		public void run() {
			StringBuilder sb = new StringBuilder(1024);
			Set<String> keySet = new HashSet<>();
			for (TraceTransferBean bean : traceTransferBeanList) {
				keySet.addAll(bean.getTransKey());
				sb.append(bean.getTransData());
			}
			sendTranDataByMQ(keySet, sb.toString(), traceTopicName);
		}

		private void sendTranDataByMQ(Set<String> keySet, final String data, String traceTopic) {
			final Message message = new Message(traceTopic, data.getBytes(StandardCharsets.UTF_8));
			message.setKeys(keySet);

			try {
				Set<String> traceBrokerSet = tryGetMessageQueueBrokerSet(traceProducer.getDefaultMQProducerImpl(), traceTopic);
				SendCallback callback = new SendCallback() {
					@Override
					public void onSuccess(SendResult sendResult) {
					}

					@Override
					public void onException(Throwable e) {
						log.error("send trace data failed, the traceData is {}", data, e);
					}
				};

				if (traceBrokerSet.isEmpty()) {
					traceProducer.send(message, callback, 5000);
				}
				else {
					traceProducer.send(message, new MessageQueueSelector() {
						@Override
						public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
							Set<String> brokerSet = (Set<String>) arg;
							List<MessageQueue> filterMqs = mqs.stream().filter(mq -> brokerSet.contains(mq.getBrokerName())).collect(Collectors.toList());
							int index = sendWhichQueue.incrementAndGet();
							int pos = index % filterMqs.size();
							return filterMqs.get(index);
						}
					}, traceBrokerSet, callback);
				}
			}
			catch (Exception e) {
				log.error("send trace data failed, the traceData is {}", data, e);
			}
		}

		private Set<String> tryGetMessageQueueBrokerSet(DefaultMQProducerImpl producer, String topic) {
			Set<String> brokerSet = new HashSet<>();
			TopicPublishInfo topicPublishInfo = producer.getTopicPublishInfoTable().get(topic);

			if (topicPublishInfo == null || !topicPublishInfo.ok()) {
				producer.getTopicPublishInfoTable().putIfAbsent(topic, new TopicPublishInfo());
				producer.getMqClientFactory().updateTopicRouteInfoFromNameServer(topic);
				topicPublishInfo = producer.getTopicPublishInfoTable().get(topic);
			}

			if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
				for (MessageQueue queue : topicPublishInfo.getMessageQueueList()) {
					brokerSet.add(queue.getBrokerName());
				}
			}
			return brokerSet;
		}

	}
}
