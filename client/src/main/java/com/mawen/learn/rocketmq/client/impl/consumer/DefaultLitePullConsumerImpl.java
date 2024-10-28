package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.mawen.learn.rocketmq.client.consumer.DefaultLitePullConsumer;
import com.mawen.learn.rocketmq.client.consumer.PullResult;
import com.mawen.learn.rocketmq.client.consumer.TopicMessageQueueChangeListener;
import com.mawen.learn.rocketmq.client.consumer.store.OffsetStore;
import com.mawen.learn.rocketmq.client.exception.MQBrokerException;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.hook.ConsumeMessageHook;
import com.mawen.learn.rocketmq.client.hook.FilterMessageHook;
import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.common.ServiceState;
import com.mawen.learn.rocketmq.common.consumer.ConsumeFromWhere;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.protocol.NamespaceUtil;
import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import com.mawen.learn.rocketmq.remoting.protocol.filter.FilterAPI;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.MessageModel;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/28
 */
@Getter
public class DefaultLitePullConsumerImpl implements MQConsumerInner{

	private static final Logger log = LoggerFactory.getLogger(DefaultLitePullConsumerImpl.class);

	private static final String NOT_RUNNING_EXCEPTION_MESSAGE = "The consumer not running, please start it first.";
	private static final String SUBSCRIPTION_CONFLICT_EXCEPTION_MESSAGE = "Subscribe and assign are mutually exclusive.";
	private static final long PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL = 50;
	private static final long PULL_TIME_DELAY_MILLIS_WHEN_BROKER_FLOW_CONTROL = 20;
	private static final long PULL_TIME_DELAY_MILLIS_WHEN_PAUSE = 1000;
	private static final long PULL_TIME_DELAY_MILLS_ON_EXCEPTION = 3 * 1000;

	private static boolean doNotUpdateTopicSubscribeInfoWhenSubscriptionChanged = false;

	private final long consumerStartTimestamp = System.currentTimeMillis();
	private final RPCHook rpcHook;
	private final List<FilterMessageHook> filterMessageHookList = new ArrayList<>();
	private volatile ServiceState serviceState = ServiceState.CREATE_JUST;
	private RebalanceImpl rebalanceImpl = new RebalanceLitePullImpl(this);

	protected MQClientInstance mqClientInstance;
	private PullAPIWapper pullAPIWapper;
	private OffsetStore offsetStore;
	private long pullTimeDelayMillisWhenException = 1000;
	private ConcurrentMap<String, String> topicToSubExpression = new ConcurrentHashMap<>();
	private DefaultLitePullConsumer defaultLitePullConsumer;
	private final ConcurrentMap<MessageQueue, PullTaskImpl> taskTable = new ConcurrentHashMap<>();
	private AssignedMessageQueue assignedMessageQueue = new AssignedMessageQueue();
	private final BlockingQueue<ConsumeRequest> consumeRequestCache = new LinkedBlockingDeque<>();
	private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;
	private final ScheduledExecutorService scheduledExecutorService;
	private Map<String, TopicMessageQueueChangeListener> topicMessageQueueChangeListenerMap = new HashMap<>();
	private Map<String, Set<MessageQueue>> messageQueuesForTopic = new HashMap<>();
	private long consumeRequestFlowControlTimes = 0L;
	private long queueFlowControlTimes = 0L;
	private long queueMaxSpanFlowControlTimes = 0L;
	private long nextAutoCommitDeadline = -1L;
	private final MessageQueueLock messageQueueLock = new MessageQueueLock();
	private final List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<>();

	@Override
	public String groupName() {
		return "";
	}

	@Override
	public MessageModel messageModel() {
		return null;
	}

	@Override
	public ConsumeType consumeType() {
		return null;
	}

	@Override
	public ConsumeFromWhere consumeFromWhere() {
		return null;
	}

	@Override
	public Set<SubscriptionData> subscriptions() {
		return Set.of();
	}

	@Override
	public void doBalance() {

	}

	@Override
	public boolean tryBalance() {
		return false;
	}

	@Override
	public void persistConsumerOffset() {

	}

	@Override
	public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {

	}

	@Override
	public boolean isSubscribeTopicNeedUpdate(String topic) {
		return false;
	}

	@Override
	public boolean isUnitMode() {
		return false;
	}

	@Override
	public ConsumerRunningInfo consumerRunningInfo() {
		return null;
	}

	public synchronized void registerTopicMessageQueueChangeListener(String topic, TopicMessageQueueChangeListener listener) throws MQClientException {
		if (topic == null || listener == null) {
			throw new MQClientException("Topi or listener is null", null);
		}
		if (topicMessageQueueChangeListenerMap.containsKey(topic)) {
			log.warn("Topic {} had been registered, new listener will overwrite the old one", topic);
		}

		topicMessageQueueChangeListenerMap.put(topic, listener);
		if (this.serviceState == ServiceState.RUNNING) {
			Set<MessageQueue> messageQueues = fetchMessageQueues(topic);
			messageQueuesForTopic.put(topic, messageQueues);
		}
	}

	private synchronized void fetchTopicMessageQueuesAndCompare() {
		for (Map.Entry<String, TopicMessageQueueChangeListener> entry : topicMessageQueueChangeListenerMap.entrySet()) {
			String topic = entry.getKey();
			TopicMessageQueueChangeListener topicMessageQueueChangeListener = entry.getValue();

			Set<MessageQueue> oldMessageQueues = messageQueuesForTopic.get(topic);
			Set<MessageQueue> newMessageQueues = fetchMessageQueues(topic);

			boolean isChanged = !isSetEqual(oldMessageQueues, newMessageQueues);

		}
	}

	private boolean isSetEqual(Set<MessageQueue> set1, Set<MessageQueue> set2) {
		if (set1 == null && set2 == null) {
			return true;
		}

		if (set1 == null || set2 == null || set1.size() != set2.size()) {
			return false;
		}

		for (MessageQueue mq : set2) {
			if (!set1.contains(mq)) {
				return false;
			}
		}

		return true;
	}

	private Set<MessageQueue> parseMessageQueues(Set<MessageQueue> queueSet) {
		Set<MessageQueue> resultQueues = new HashSet<>();
		for (MessageQueue mq : queueSet) {
			String userTopic = NamespaceUtil.withoutNamespace(mq.getTopic(), defaultLitePullConsumer.getNamespace());
			resultQueues.add(new MessageQueue(userTopic, mq.getBrokerName(), mq.getQueueId()));
		}
		return resultQueues;
	}

	private enum SubscriptionType {
		NONE, SUBSCRIBE, ASSIGN;
	}

	@Getter
	@Setter
	@RequiredArgsConstructor
	public class PullTaskImpl implements Runnable {

		private final MessageQueue mq;
		private volatile boolean cancelled = false;
		private Thread currentThread;

		public void tryInterrupt() {
			setCancelled(true);
			if (currentThread == null) {
				return;
			}
			if (!currentThread.isInterrupted()) {
				currentThread.interrupt();
			}
		}

		@Override
		public void run() {
			if (!this.isCancelled()) {
				this.currentThread = Thread.currentThread();

				if (assignedMessageQueue.isPaused(mq)) {
					scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLIS_WHEN_PAUSE, TimeUnit.MILLISECONDS);
					log.debug("Message Queue: {} has been paused!", mq);
					return;
				}

				ProcessQueue pq = assignedMessageQueue.getProcessQueue(mq);
				if (pq == null || pq.isDropped()) {
					log.info("The message queue not be able to poll, because it's dropped. group={}, messageQueue={}", defaultLitePullConsumer.getConsumerGroup(), this.mq);
					return;
				}

				pq.setLastPullTimestamp(System.currentTimeMillis());

				if ((long) consumeRequestCache.size() * defaultLitePullConsumer.getPullBatchSize() > defaultLitePullConsumer.getPullThresholdForAll()) {
					scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL, TimeUnit.MILLISECONDS);
					if ((consumeRequestFlowControlTimes++ % 1000) == 0) {
						log.warn("The consume request count exceeds threshold {}, so do flow control, consume request count={}, flowControlTimes={}",
								Math.ceil((double) defaultLitePullConsumer.getPullThresholdForAll() / defaultLitePullConsumer.getPullBatchSize()), consumeRequestCache.size(), consumeRequestFlowControlTimes);
					}
					return;
				}

				long cachedMessageCount = pq.getMsgCount().get();
				long cachedMessageSizeInMiB = pq.getMsgSize().get() / (1024 * 1024);
				if (cachedMessageCount > defaultLitePullConsumer.getPullThresholdForQueue()) {
					scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL, TimeUnit.MILLISECONDS);
					if (queueFlowControlTimes++ % 1000 == 0) {
						log.warn("The cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, flowControlTimes={}",
								defaultLitePullConsumer.getPullThresholdForQueue(), pq.getMsgTreeMap().firstEntry(), pq.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, queueFlowControlTimes);
					}
					return;
				}

				if (cachedMessageSizeInMiB > defaultLitePullConsumer.getPullThresholdSizeForQueue()) {
					scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL, TimeUnit.MILLISECONDS);
					if (queueFlowControlTimes++ % 1000 == 0) {
						log.warn("The cache message size exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, flowControlTimes={}",
								defaultLitePullConsumer.getPullThresholdSizeForQueue(), pq.getMsgTreeMap().firstKey(), pq.getMsgTreeMap().lastKey(), pq.getMaxSpan(), queueMaxSpanFlowControlTimes);
					}
					return;
				}

				long offset = 0L;
				try {
					offset = nextPullOffset(mq);
				}
				catch (Exception e) {
					log.error("Failed to get next pull offset", e);
					scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_ON_EXCEPTION, TimeUnit.MILLISECONDS);
					return;
				}

				if (this.isCancelled() || pq.isDropped()) {
					return;
				}
				long pullDelayTimeMillis = 0L;
				try {
					SubscriptionData subscriptionData;
					String topic = this.mq.getTopic();
					if (subscriptionType == SubscriptionType.SUBSCRIBE) {
						subscriptionData = rebalanceImpl.getSubscriptionInner().get(topic);
					}
					else {
						String subExpression4Assign = topicToSubExpression.get(topic);
						subExpression4Assign = subExpression4Assign == null ? SubscriptionData.SUB_ALL : subExpression4Assign;
						subscriptionData = FilterAPI.buildSubscriptionData(topic, subExpression4Assign);
					}

					PullResult pullResult = pull(mq, subscriptionData, offset, defaultLitePullConsumer.getPullBatchSize());
					if (this.isCancelled() || pq.isDropped()) {
						return;
					}
					switch (pullResult.getPullStatus()) {
						case FOUND:
							Object objLock = messageQueueLock.fetchLockObject(mq);
							synchronized (objLock) {
								if (pullResult.getMsgFoundList() != null && !pullResult.getMsgFoundList().isEmpty() && assignedMessageQueue.getSeekOffset(mq) == -1) {
									pq.putMessage(pullResult.getMsgFoundList());
									submitConsumeRequest(new ConsumeRequest(pullResult.getMsgFoundList(), mq, pq));
								}
							}
							break;
						case OFFSET_ILLEGAL:
							log.warn("The pull request offset illegal, {}", pullResult);
							break;
						default:
							break;
					}
					updatePullResult(mq, pullResult.getNextBeginOffset(), pq);
				}
				catch (InterruptedException e) {
					log.warn("Polling thread was interrupted.", e);
				}
				catch (Throwable e) {
					if (e instanceof MQBrokerException && ((MQBrokerException)e).getResponseCode() == ResponseCode.FLOW_CONTROL) {
						pullDelayTimeMillis = PULL_TIME_DELAY_MILLIS_WHEN_BROKER_FLOW_CONTROL;
					}
					else {
						pullDelayTimeMillis = pullTimeDelayMillisWhenException;
					}
				}

				if (!this.isCancelled()) {
					scheduledThreadPoolExecutor.schedule(this, pullDelayTimeMillis, TimeUnit.MILLISECONDS);
				}
				else {
					log.warn("The Pull Task is cancelled after doPullTask, {}", mq);
				}
			}
		}
	}

	@AllArgsConstructor
	@Getter
	public class ConsumeRequest {
		private final List<MessageExt> messageExts;
		private final MessageQueue messageQueues;
		private final ProcessQueue processQueue;
	}
}
