package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.mawen.learn.rocketmq.client.consumer.DefaultLitePullConsumer;
import com.mawen.learn.rocketmq.client.consumer.PullResult;
import com.mawen.learn.rocketmq.client.consumer.TopicMessageQueueChangeListener;
import com.mawen.learn.rocketmq.client.consumer.store.OffsetStore;
import com.mawen.learn.rocketmq.client.consumer.store.ReadOffsetType;
import com.mawen.learn.rocketmq.client.exception.MQBrokerException;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.hook.ConsumeMessageHook;
import com.mawen.learn.rocketmq.client.hook.FilterMessageHook;
import com.mawen.learn.rocketmq.client.impl.CommunicationMode;
import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.ServiceState;
import com.mawen.learn.rocketmq.common.ThreadFactoryImpl;
import com.mawen.learn.rocketmq.common.consumer.ConsumeFromWhere;
import com.mawen.learn.rocketmq.common.filter.ExpressionType;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.common.sysflag.PullSysFlag;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingConnectException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingSendRequestException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTimeoutException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.mawen.learn.rocketmq.remoting.protocol.NamespaceUtil;
import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.ProcessQueueInfo;
import com.mawen.learn.rocketmq.remoting.protocol.filter.FilterAPI;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.MessageModel;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/28
 */
@Setter
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

	protected MQClientInstance mqClientFactory;
	private PullAPIWrapper pullAPIWrapper;
	private OffsetStore offsetStore;
	private SubscriptionType subscriptionType = SubscriptionType.NONE;
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

	public DefaultLitePullConsumerImpl(final DefaultLitePullConsumer defaultLitePullConsumer, RPCHook rpcHook) {
		this.defaultLitePullConsumer = defaultLitePullConsumer;
		this.rpcHook = rpcHook;
		this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(this.defaultLitePullConsumer.getPullThreadNums(),
				new ThreadFactoryImpl("PullMsgThread-" + this.defaultLitePullConsumer.getConsumerGroup()));
		this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("MonitorMessageQueueChangeThread"));
		this.pullTimeDelayMillisWhenException = defaultLitePullConsumer.getPullTimeDelayMillisWhenException();
	}

	public void updateConsumeOffset(MessageQueue mq, long offset) {
		checkServiceState();
		this.offsetStore.updateOffset(mq, offset, false);
	}

	public long committed(MessageQueue mq) {
		checkServiceState();

	}

	public long searchOffset(MessageQueue mq, long timestamp) {
		checkServiceState();
		return this.mqClientFactory.getMqAdminImpl().searchOffset(mq, timestamp);
	}

	@Override
	public String groupName() {
		return this.defaultLitePullConsumer.getConsumerGroup();
	}

	@Override
	public MessageModel messageModel() {
		return this.defaultLitePullConsumer.getMessageModel();
	}

	@Override
	public ConsumeType consumeType() {
		return ConsumeType.CONSUME_ACTIVELY;
	}

	@Override
	public ConsumeFromWhere consumeFromWhere() {
		return ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
	}

	@Override
	public Set<SubscriptionData> subscriptions() {
		Set<SubscriptionData> subSet = new HashSet<>();
		subSet.addAll(this.rebalanceImpl.getSubscriptionInner().values());
		return subSet;
	}

	@Override
	public void doRebalance() {
		if (this.rebalanceImpl != null) {
			this.rebalanceImpl.doRebalance(false);
		}
	}

	@Override
	public boolean tryBalance() {
		if (this.rebalanceImpl != null) {
			return this.rebalanceImpl.doRebalance(false);
		}
		return false;
	}

	@Override
	public void persistConsumerOffset() {
		try {
			checkServiceState();
			Set<MessageQueue> mqs = new HashSet<>();
			if (this.subscriptionType == SubscriptionType.SUBSCRIBE) {
				Set<MessageQueue> allocateMQ = this.rebalanceImpl.getProcessQueueTable().keySet();
				mqs.addAll(allocateMQ);
			}
			else if (this.subscriptionType == SubscriptionType.ASSIGN) {
				Set<MessageQueue> assignedMessageQueues = this.assignedMessageQueue.getAssignedMessageQueues();
				mqs.addAll(assignedMessageQueues);
			}
			this.offsetStore.persistAll(mqs);
		}
		catch (Exception e) {
			log.error("Persist consumer offset error for group: {}", this.defaultLitePullConsumer.getConsumerGroup());
		}
	}

	@Override
	public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
		ConcurrentMap<String, SubscriptionData> subTable = this.rebalanceImpl.getSubscriptionInner();
		if (subTable != null) {
			if (subTable.containsKey(topic)) {
				this.rebalanceImpl.getTopicSubscribeInfoTable().put(topic, info);
			}
		}
	}

	@Override
	public boolean isSubscribeTopicNeedUpdate(String topic) {
		ConcurrentMap<String, SubscriptionData> subTable = this.rebalanceImpl.getSubscriptionInner();
		if (subTable != null) {
			if (subTable.containsKey(topic)) {
				return !this.rebalanceImpl.topicSubscribeInfoTable.containsKey(topic);
			}
		}
		return false;
	}

	@Override
	public boolean isUnitMode() {
		return this.defaultLitePullConsumer.isUnitMode();
	}

	@Override
	public ConsumerRunningInfo consumerRunningInfo() {
		ConsumerRunningInfo info = new ConsumerRunningInfo();
		Properties prop = MixAll.object2Properties(this.defaultLitePullConsumer);
		prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, String.valueOf(this.consumerStartTimestamp));
		info.setProperties(prop);

		info.getSubscriptionSet().addAll(this.subscriptions());

		for (MessageQueue mq : this.assignedMessageQueue.getAssignedMessageQueues()) {
			ProcessQueue pq = this.assignedMessageQueue.getProcessQueue(mq);
			ProcessQueueInfo pqInfo = new ProcessQueueInfo();
			pqInfo.setCommitOffset(this.offsetStore.readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE));
			pq.fillProcessQueueInfo(pqInfo);
			info.getMqTable().put(mq, pqInfo);
		}

		return info;
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

	private void checkServiceState() {
		if (this.serviceState != ServiceState.RUNNING) {
			throw new IllegalStateException(NOT_RUNNING_EXCEPTION_MESSAGE);
		}
	}

	public Set<MessageQueue> fetchMessageQueues(String topic) throws MQClientException {
		checkServiceState();
		Set<MessageQueue> result = this.mqClientFactory.getMqAdminImpl().fetchSubscribeMessageQueues(topic);
		return parseMessageQueues(result);
	}

	private long fetchConsumeOffset(MessageQueue mq) throws MQClientException {
		checkServiceState();
		return this.rebalanceImpl.computePullFromWhereWithException(mq);
	}

	private void clearMessageQueueInCache(MessageQueue mq) {
		ProcessQueue pq = assignedMessageQueue.getProcessQueue(mq);
		if (pq != null) {
			pq.clear();
		}

		Iterator<ConsumeRequest> it = consumeRequestCache.iterator();
		while (it.hasNext()) {
			if (it.next().getMessageQueues().equals(mq)) {
				it.remove();
			}
		}
	}

	private long nextPullOffset(MessageQueue mq) {
		long offset = -1;
		long seekOffset = assignedMessageQueue.getSeekOffset(mq);
		if (seekOffset == -1) {
			offset = seekOffset;
			assignedMessageQueue.updateConsumeOffset(mq, offset);
			assignedMessageQueue.setSeekOffset(mq, -1);
		}
		else {
			offset = assignedMessageQueue.getPullOffset(mq);
			if (offset == -1) {
				offset = fetchConsumeOffset(mq);
			}
		}
		return offset;
	}

	private PullResult pull(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException, MQBrokerException, InterruptedException, MQClientException, RemotingTooMuchRequestException {
		return pull(mq, subscriptionData, offset, maxNums, this.defaultLitePullConsumer.getConsumerPullTimeoutMillis());
	}

	private PullResult pull(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums, long timeout) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException, MQBrokerException, InterruptedException, MQClientException, RemotingTooMuchRequestException {
		return this.pullSyncImpl(mq, subscriptionData, offset, maxNums, true, timeout);
	}

	private PullResult pullSyncImpl(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums, boolean block, long timeout) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException, MQBrokerException, InterruptedException, RemotingTooMuchRequestException {
		if (mq == null) {
			throw new MQClientException("mq is null", null);
		}
		if (offset < 0) {
			throw new MQClientException("offset < 0", null);
		}
		if (maxNums <= 0) {
			throw new MQClientException("maxNums <= 0", null);
		}

		int sysFlag = PullSysFlag.buildSysFlag(false, block, true, false);
		long timeoutMillis = block ? this.defaultLitePullConsumer.getConsumerTimeoutMillisWhenSuspend() : timeout;
		boolean isTagType = ExpressionType.isTagType(subscriptionData.getExpressionType());

		PullResult pullResult = this.pullAPIWrapper.pullKernelImpl(mq, subscriptionData.getSubString(), subscriptionData.getExpressionType(),
				isTagType ? 0L : subscriptionData.getSubVersion(), offset, maxNums, sysFlag, 0, this.defaultLitePullConsumer.getBrokerSuspendMaxTimeMillis(),
				timeoutMillis, CommunicationMode.SYNC, null);

		this.pullAPIWrapper.processPullResult(mq, pullResult, subscriptionData);

		return pullResult;
	}

	private void resetTopic(List<MessageExt> msgList) {
		if (CollectionUtils.isEmpty(msgList)) {
			return;
		}

		if (this.defaultLitePullConsumer.getNamespace() != null) {
			for (MessageExt msg : msgList) {
				msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultLitePullConsumer.getNamespace()));
			}
		}
	}

	private void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
		this.offsetStore.updateConsumeOffsetToBroker(mq, offset, isOneway);
	}

	private synchronized void fetchTopicMessageQueuesAndCompare() throws MQClientException {
		for (Map.Entry<String, TopicMessageQueueChangeListener> entry : topicMessageQueueChangeListenerMap.entrySet()) {
			String topic = entry.getKey();
			TopicMessageQueueChangeListener topicMessageQueueChangeListener = entry.getValue();

			Set<MessageQueue> oldMessageQueues = messageQueuesForTopic.get(topic);
			Set<MessageQueue> newMessageQueues = fetchMessageQueues(topic);

			boolean isChanged = !isSetEqual(oldMessageQueues, newMessageQueues);
			if (isChanged) {
				messageQueuesForTopic.put(topic, newMessageQueues);
				if (topicMessageQueueChangeListener != null) {
					topicMessageQueueChangeListener.onChanged(topic, newMessageQueues);
				}
			}
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
