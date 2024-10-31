package com.mawen.learn.rocketmq.client.impl.consumer;

import java.sql.Time;
import java.util.ArrayList;
import java.util.Collection;
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

import com.mawen.learn.rocketmq.client.Validators;
import com.mawen.learn.rocketmq.client.consumer.DefaultLitePullConsumer;
import com.mawen.learn.rocketmq.client.consumer.MessageQueueListener;
import com.mawen.learn.rocketmq.client.consumer.MessageSelector;
import com.mawen.learn.rocketmq.client.consumer.PullResult;
import com.mawen.learn.rocketmq.client.consumer.TopicMessageQueueChangeListener;
import com.mawen.learn.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.mawen.learn.rocketmq.client.consumer.store.LocalFileOffsetStore;
import com.mawen.learn.rocketmq.client.consumer.store.OffsetStore;
import com.mawen.learn.rocketmq.client.consumer.store.ReadOffsetType;
import com.mawen.learn.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import com.mawen.learn.rocketmq.client.exception.MQBrokerException;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.hook.ConsumeMessageContext;
import com.mawen.learn.rocketmq.client.hook.ConsumeMessageHook;
import com.mawen.learn.rocketmq.client.hook.FilterMessageHook;
import com.mawen.learn.rocketmq.client.impl.CommunicationMode;
import com.mawen.learn.rocketmq.client.impl.MQClientManager;
import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.ServiceState;
import com.mawen.learn.rocketmq.common.ThreadFactoryImpl;
import com.mawen.learn.rocketmq.common.consumer.ConsumeFromWhere;
import com.mawen.learn.rocketmq.common.filter.ExpressionType;
import com.mawen.learn.rocketmq.common.help.FAQUrl;
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
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
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

	public long committed(MessageQueue mq) throws MQClientException {
		checkServiceState();
		long offset = this.offsetStore.readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
		if (offset == -2) {
			throw new MQClientException("Fetch consume offset from broker exception", null);
		}
		return offset;
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

	public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
		consumeMessageHookList.add(hook);
		log.info("register consumeMessageHook hook, {}", hook.hookName());
	}

	public void executeHookBefore(final ConsumeMessageContext context) {
		if (!consumeMessageHookList.isEmpty()) {
			for (ConsumeMessageHook hook : consumeMessageHookList) {
				try {
					hook.consumeMessageBefore(context);
				}
				catch (Exception e) {
					log.error("consumeMessageHook {} executeHookBefore exception", hook.hookName(), e);
				}
			}
		}
	}

	public void executeHookAfter(final ConsumeMessageContext context) {
		if (!consumeMessageHookList.isEmpty()) {
			for (ConsumeMessageHook hook : consumeMessageHookList) {
				try {
					hook.consumeMessageAfter(context);
				}
				catch (Exception e) {
					log.error("consumeMessageHook {} executeHookAfter exception", hook.hookName(), e);
				}
			}
		}
	}

	public void updateNameServerAddress(String newAddress) {
		mqClientFactory.getMqClientAPIImpl().updateNameServerAddressList(newAddress);
	}

	public void updateAssignQueueAndStartPullTask(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
		MessageModel messageModel = defaultLitePullConsumer.getMessageModel();
		switch (messageModel) {
			case BROADCASTING:
				updateAssignedMessageQueue(topic, mqAll);
				updatePullTask(topic, mqAll);
				break;
			case CLUSTERING:
				updateAssignedMessageQueue(topic, mqDivided);
				updatePullTask(topic, mqDivided);
				break;
			default:
				break;
		}
	}

	public synchronized void subscribe(String topic, String subExpression, MessageQueueListener messageQueueListener) throws MQClientException {
		try {
			if (StringUtils.isBlank(topic)) {
				throw new IllegalArgumentException("Topic can not be null or empty");
			}

			setSubscriptionType(SubscriptionType.SUBSCRIBE);
			SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subExpression);
			rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
			defaultLitePullConsumer.setMessageQueueListener(new MessageQueueListener() {
				@Override
				public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqAssigned) {
					updateAssignQueueAndStartPullTask(topic, mqAll, mqAssigned);
					messageQueueListener.messageQueueChanged(topic, mqAll, mqAssigned);
				}
			});
			assignedMessageQueue.setRebalanceImpl(rebalanceImpl);

			if (serviceState == ServiceState.RUNNING) {
				mqClientFactory.sendHeartbeatToAllBrokerWithLock();
				updateTopicSubscribeInfoWhenSubscriptionChanged();
			}
		}
		catch (Exception e) {
			throw new MQClientException("subscribe exception", e);
		}
	}

	public synchronized void subscribe(String topic, String subExpression) throws MQClientException {
		try {
			if (StringUtils.isBlank(topic)) {
				throw new IllegalArgumentException("Topic can not be null or empty");
			}

			setSubscriptionType(SubscriptionType.SUBSCRIBE);
			SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subExpression);
			rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
			defaultLitePullConsumer.setMessageQueueListener(new MessageQueueListenerImpl());
			assignedMessageQueue.setRebalanceImpl(rebalanceImpl);

			if (serviceState == ServiceState.RUNNING) {
				mqClientFactory.sendHeartbeatToAllBrokerWithLock();
				updateTopicSubscribeInfoWhenSubscriptionChanged();
			}
		}
		catch (Exception e) {
			throw new MQClientException("subscribe exception", e);
		}
	}

	public synchronized void subscribe(String topic, MessageSelector messageSelector) throws MQClientException {
		try {
			if (StringUtils.isBlank(topic)) {
				throw new IllegalArgumentException("Topic can not be null or empty");
			}

			setSubscriptionType(SubscriptionType.SUBSCRIBE);
			if (messageSelector == null) {
				subscribe(topic, SubscriptionData.SUB_ALL);
				return;
			}

			SubscriptionData subscriptionData = FilterAPI.build(topic, messageSelector.getExpression(), messageSelector.getType());
			rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
			defaultLitePullConsumer.setMessageQueueListener(new MessageQueueListenerImpl());
			assignedMessageQueue.setRebalanceImpl(rebalanceImpl);

			if (serviceState == ServiceState.RUNNING) {
				mqClientFactory.sendHeartbeatToAllBrokerWithLock();
				updateTopicSubscribeInfoWhenSubscriptionChanged();
			}
		}
		catch (Exception e) {
			throw new MQClientException("subscribe exception", e);
		}
	}

	public synchronized void unsubscribe(final String topic) {
		this.rebalanceImpl.getSubscriptionInner().remove(topic);
		removePullTaskCallback(topic);
		assignedMessageQueue.removeAssignedMessageQueue(topic);
	}

	public synchronized void assign(Collection<MessageQueue> mqs) {
		if (CollectionUtils.isEmpty(mqs)) {
			throw new IllegalArgumentException("Message queues can not be null or empty.");
		}

		setSubscriptionType(SubscriptionType.ASSIGN);
		assignedMessageQueue.updateAssignedMessageQueue(mqs);
		if (serviceState == ServiceState.RUNNING) {
			updateAssignPullTask(mqs);
		}
	}

	public synchronized void setSubExpressionForAssign(final String topic, final String subExpression) {
		if (StringUtils.isBlank(subExpression)) {
			throw new IllegalArgumentException("subExpression can not be null or empty.");
		}
		else if (serviceState != ServiceState.CREATE_JUST) {
			throw new IllegalStateException("setAssignTag only can be called before start.");
		}

		setSubscriptionType(SubscriptionType.ASSIGN);
		topicToSubExpression.put(topic, subExpression);
	}

	public synchronized List<MessageExt> poll(long timeout) {
		try {
			checkServiceState();
			if (timeout < 0) {
				throw new IllegalArgumentException("timeout must not be negative");
			}

			if (defaultLitePullConsumer.isAutoCommit()) {
				maybeAutoCommit();
			}

			long end = System.currentTimeMillis() + timeout;

			ConsumeRequest consumeRequest = consumeRequestCache.poll(end - System.currentTimeMillis(), TimeUnit.MILLISECONDS);

			if (end - System.currentTimeMillis() > 0) {
				while (consumeRequest != null && consumeRequest.getProcessQueue().isDropped()) {
					consumeRequest = consumeRequestCache.poll(end - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
					if (end - System.currentTimeMillis() <= 0) {
						break;
					}
				}
			}

			if (consumeRequest != null && !consumeRequest.getProcessQueue().isDropped()) {
				List<MessageExt> messageExts = consumeRequest.getMessageExts();
				long offset = consumeRequest.getProcessQueue().removeMessage(messageExts);
				assignedMessageQueue.updateConsumeOffset(consumeRequest.getMessageQueue(), offset);
				this.resetTopic(messageExts);

				if (!consumeMessageHookList.isEmpty()) {
					ConsumeMessageContext context = new ConsumeMessageContext();
					context.setNamespace(defaultLitePullConsumer.getNamespace());
					context.setConsumerGroup(groupName());
					context.setMq(consumeRequest.getMessageQueue());
					context.setMsgList(messageExts);
					context.setSuccess(false);

					executeHookBefore(context);

					context.setStatus(ConsumeConcurrentlyStatus.CONSUME_SUCCESS.toString());
					context.setSuccess(true);
					context.setAccessChannel(defaultLitePullConsumer.getAccessChannel());

					executeHookAfter(context);
				}
				consumeRequest.getProcessQueue().setLastConsumeTimestamp(System.currentTimeMillis());
				return messageExts;
			}

		}
		catch (InterruptedException ignored) {}

		return Collections.emptyList();
	}

	public void pause(Collection<MessageQueue> mqs) {
		assignedMessageQueue.pause(mqs);
	}

	public void resume(Collection<MessageQueue> mqs) {
		assignedMessageQueue.resume(mqs);
	}

	public synchronized void seek(MessageQueue mq, long offset) throws MQClientException {
		if (!assignedMessageQueue.getAssignedMessageQueues().contains(mq)) {
			if (subscriptionType == SubscriptionType.SUBSCRIBE) {
				throw new MQClientException("The message queue is not in assigned list, maybe rebalancing, message queue " + mq, null);
			}
			else {
				throw new MQClientException("The message queue is not in assigned list, message queue " + mq, null);
			}
		}

		long minOffset = minOffset(mq);
		long maxOffset = maxOffset(mq);
		if (offset < minOffset || offset > maxOffset) {
			throw new MQClientException("Seek offset illegal, seek offset =" + offset + ", min offset = " + minOffset + ", max offset = " + maxOffset, null);
		}

		Object objLock = messageQueueLock.fetchLockObject(mq);
		synchronized (objLock) {
			clearMessageQueueInCache(mq);

			PullTaskImpl oldPullTask = taskTable.get(mq);
			if (oldPullTask != null) {
				oldPullTask.tryInterrupt();
				taskTable.remove(mq);
			}

			assignedMessageQueue.setSeekOffset(mq, offset);
			if (!taskTable.containsKey(mq)) {
				PullTaskImpl pullTask = new PullTaskImpl(mq);
				taskTable.put(mq, pullTask);
				scheduledExecutorService.schedule(pullTask, 0, TimeUnit.MILLISECONDS);
			}
		}
	}

	public void seekToBegin(MessageQueue mq) throws MQClientException {
		long begin = minOffset(mq);
		this.seek(mq, begin);
	}

	public void seekToEnd(MessageQueue mq) throws MQClientException {
		long end = maxOffset(mq);
		this.seek(mq, end);
	}

	public synchronized void commitAll() {
		for (MessageQueue mq : assignedMessageQueue.getAssignedMessageQueues()) {
			try {
				commit(mq);
			}
			catch (Exception e) {
				log.error("An error occurred when update consume offset Automatically");
			}
		}
	}

	public synchronized void commit(final Map<MessageQueue, Long> mqMap, boolean persist) {
		if (MapUtils.isEmpty(mqMap)) {
			log.warn("MessageQueue is empty, ignore this commit");
			return;
		}

		for (Map.Entry<MessageQueue, Long> entry : mqMap.entrySet()) {
			MessageQueue mq = entry.getKey();
			Long offset = entry.getValue();

			if (offset != -1) {
				ProcessQueue pq = assignedMessageQueue.getProcessQueue(mq);
				if (pq != null && !pq.isDropped()) {
					updateConsumeOffset(mq, offset);
				}
			}
			else {
				log.error("consumerOffset is -1 in messageQueue[{}]", mq);
			}
		}
	}

	public synchronized Set<MessageQueue> assignment() {
		return assignedMessageQueue.getAssignedMessageQueues();
	}

	public synchronized void commit(final Set<MessageQueue> mqs, boolean persist) {
		if (CollectionUtils.isEmpty(mqs)) {
			return;
		}

		mqs.forEach(this::commit);

		if (persist) {
			offsetStore.persistAll(mqs);
		}
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

	public synchronized boolean isRunning() {
		return serviceState == ServiceState.RUNNING;
	}

	public synchronized void start() throws MQClientException {
		switch (serviceState) {
			case CREATE_JUST:
				serviceState = ServiceState.START_FAILED;

				checkConfig();

				if (defaultLitePullConsumer.getMessageModel() == MessageModel.CLUSTERING) {
					defaultLitePullConsumer.changeInstanceNameToPID();
				}

				initMQClientFactory();

				initRebalanceImpl();

				initPullAPIWrapper();

				initOffsetStore();

				mqClientFactory.start();

				startScheduleTask();

				serviceState = ServiceState.RUNNING;

				log.info("the consumer[{}] start OK", groupName());

				operateAfterRunning();
				break;
			case RUNNING:
			case START_FAILED:
			case SHUTDOWN_ALREADY:
				throw new MQClientException("The PullConsumer service state not OK, maybe started once," + this.serviceState
						+ FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK), null);
			default:
				break;
		}
	}

	public synchronized void shutdown() {
		switch (this.serviceState) {
			case CREATE_JUST:
				break;
			case RUNNING:
				persistConsumerOffset();

				mqClientFactory.unregisterConsumer(groupName());

				scheduledExecutorService.shutdown();

				scheduledThreadPoolExecutor.shutdown();

				mqClientFactory.shutdown();

				serviceState = ServiceState.SHUTDOWN_ALREADY;

				log.info("the consumer[{}] shutdown OK", groupName());
				break;
			default:
				break;
		}
	}

	private void checkConfig() throws MQClientException {
		Validators.checkGroup(groupName());

		if (groupName().equals(MixAll.DEFAULT_CONSUMER_GROUP)) {
			throw new MQClientException("ConsumerGroup can not equal " + MixAll.DEFAULT_CONSUMER_GROUP + ", please specify another one." + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
		}

		if (defaultLitePullConsumer.getMessageModel() == null) {
			throw new MQClientException("messageModel is null" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
		}

		if (defaultLitePullConsumer.getAllocateMessageQueueStrategy() == null) {
			throw new MQClientException("allocateMessageQueueStrategy is null" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
		}

		if (defaultLitePullConsumer.getConsumerTimeoutMillisWhenSuspend() < defaultLitePullConsumer.getBrokerSuspendMaxTimeMillis()) {
			throw new MQClientException("Long polling mode, the consumer consumerTimeoutMillisWhenSuspend must greater that the brokerSuspendMaxTimeMillis" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
		}

	}

	private void updateAssignedMessageQueue(String topic, Set<MessageQueue> mqs) {
		assignedMessageQueue.updateAssignedMessageQueue(mqs);
	}

	private synchronized void setSubscriptionType(SubscriptionType type) {
		if (subscriptionType == SubscriptionType.NONE) {
			subscriptionType = type;
		}
		else if (subscriptionType != type) {
			throw new IllegalStateException(SUBSCRIPTION_CONFLICT_EXCEPTION_MESSAGE);
		}
	}

	private void updatePullTask(String topic, Set<MessageQueue> mqs) {
		Iterator<Map.Entry<MessageQueue, PullTaskImpl>> iterator = taskTable.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<MessageQueue, PullTaskImpl> next = iterator.next();
			if (next.getKey().getTopic().equals(topic)) {
				if (mqs.contains(next.getKey())) {
					next.getValue().setCancelled(true);
					iterator.remove();
				}
			}
		}

		startPullTask(mqs);
	}

	private void initMQClientFactory() throws MQClientException {
		mqClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(defaultLitePullConsumer, rpcHook);
		boolean registerOK = mqClientFactory.registerConsumer(groupName(), this);
		if (!registerOK) {
			serviceState = ServiceState.CREATE_JUST;

			throw new MQClientException("The consumer group[" + groupName() + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL), null);
		}
	}

	private void initRebalanceImpl() {
		rebalanceImpl.setConsumerGroup(groupName());
		rebalanceImpl.setMessageModel(defaultLitePullConsumer.getMessageModel());
		rebalanceImpl.setAllocateMessageQueueStrategy(defaultLitePullConsumer.getAllocateMessageQueueStrategy());
		rebalanceImpl.setMqClientFactory(mqClientFactory);
	}

	private void initPullAPIWrapper() {
		pullAPIWrapper = new PullAPIWrapper(mqClientFactory, groupName(), isUnitMode());
		pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);
	}

	private void initOffsetStore() throws MQClientException {
		if (defaultLitePullConsumer.getOffsetStore() != null) {
			offsetStore = defaultLitePullConsumer.getOffsetStore();
		}
		else {
			switch (defaultLitePullConsumer.getMessageModel()) {
				case BROADCASTING:
					offsetStore = new LocalFileOffsetStore(mqClientFactory, groupName());
					break;
				case CLUSTERING:
					offsetStore = new RemoteBrokerOffsetStore(mqClientFactory, groupName());
					break;
				default:
					break;
			}
			defaultLitePullConsumer.setOffsetStore(offsetStore);
		}
		offsetStore.load();
	}

	private void startScheduleTask() {
		scheduledExecutorService.scheduleAtFixedRate(() -> {
			try {
				fetchTopicMessageQueuesAndCompare();
			}
			catch (Exception e) {
				log.error("ScheduleTask fetchTopicMessageQueuesAndCompare exception", e);
			}
		}, 10 * 1000, getDefaultLitePullConsumer().getTopicMetadataCheckIntervalMillis(), TimeUnit.MILLISECONDS);
	}

	private void operateAfterRunning() throws MQClientException {
		if (subscriptionType == SubscriptionType.SUBSCRIBE) {
			updateTopicSubscribeInfoWhenSubscriptionChanged();
		} else if (subscriptionType == SubscriptionType.ASSIGN) {
			updateAssignPullTask(assignedMessageQueue.getAssignedMessageQueues());
		}

		for (String topic : topicMessageQueueChangeListenerMap.keySet()) {
			Set<MessageQueue> mqs = fetchMessageQueues(topic);
			messageQueuesForTopic.put(topic, mqs);
		}

		mqClientFactory.checkClientInBroker();
	}

	private void startPullTask(Collection<MessageQueue> mqs) {
		for (MessageQueue mq : mqs) {
			if (taskTable.containsKey(mq)) {
				PullTaskImpl pullTask = new PullTaskImpl(mq);
				taskTable.put(mq, pullTask);
				scheduledExecutorService.schedule(pullTask, 0, TimeUnit.MILLISECONDS);
			}
		}
	}

	private void updateAssignPullTask(Collection<MessageQueue> mqs) {
		Iterator<Map.Entry<MessageQueue, PullTaskImpl>> iterator = taskTable.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<MessageQueue, PullTaskImpl> next = iterator.next();
			if (!mqs.contains(next.getKey())) {
				next.getValue().setCancelled(true);
				iterator.remove();
			}
		}

		startPullTask(mqs);
	}

	private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
		if (doNotUpdateTopicSubscribeInfoWhenSubscriptionChanged) {
			return;
		}

		ConcurrentMap<String, SubscriptionData> subTable = rebalanceImpl.getSubscriptionInner();
		if (subTable != null) {
			subTable.keySet().forEach(mqClientFactory::updateTopicRouteInfoFromNameServer);
		}
	}

	private void maybeAutoCommit() {
		long now = System.currentTimeMillis();
		if (now >= nextAutoCommitDeadline) {
			commitAll();
			nextAutoCommitDeadline = now + defaultLitePullConsumer.getAutoCommitIntervalMillis();
		}
	}

	private long maxOffset(MessageQueue mq) throws MQClientException {
		checkServiceState();
		return mqClientFactory.getMqAdminImpl().maxOffset(mq);
	}

	private long minOffset(MessageQueue mq) throws MQClientException {
		checkServiceState();
		return mqClientFactory.getMqAdminImpl().minOffset(mq);
	}

	private void removePullTaskCallback(final String topic) {
		removePullTask(topic);
	}

	private void removePullTask(final String topic) {
		Iterator<Map.Entry<MessageQueue, PullTaskImpl>> iterator = this.taskTable.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<MessageQueue, PullTaskImpl> next = iterator.next();
			if (next.getKey().getTopic().equals(topic)) {
				iterator.remove();
			}
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

	private synchronized void commit(MessageQueue mq) {
		long consumerOffset = assignedMessageQueue.getConsumerOffset(mq);

		if (consumerOffset != -1) {
			ProcessQueue pq = assignedMessageQueue.getProcessQueue(mq);
			if (pq != null && !pq.isDropped()) {
				updateConsumeOffset(mq, consumerOffset);
			}
		}
		else {
			log.error("consumerOffset is -1 in messageQueue [{}]", mq);
		}
	}

	private void updatePullOffset(MessageQueue mq, long nextPullOffset, ProcessQueue pq) {
		if (assignedMessageQueue.getSeekOffset(mq) == -1) {
			assignedMessageQueue.updatePullOffset(mq, nextPullOffset, pq);
		}
	}

	private void submitConsumeRequest(ConsumeRequest consumeRequest) {
		try {
			consumeRequestCache.put(consumeRequest);
		}
		catch (InterruptedException e) {
			log.error("Submit consumeRequest error", e);
		}
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
			if (it.next().getMessageQueue().equals(mq)) {
				it.remove();
			}
		}
	}

	private long nextPullOffset(MessageQueue mq) throws MQClientException {
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
					updatePullOffset(mq, pullResult.getNextBeginOffset(), pq);
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
		private final MessageQueue messageQueue;
		private final ProcessQueue processQueue;
	}

	class MessageQueueListenerImpl implements MessageQueueListener {
		@Override
		public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqAssigned) {
			updateAssignQueueAndStartPullTask(topic, mqAll, mqAssigned);
		}
	}
}
