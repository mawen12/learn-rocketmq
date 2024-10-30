package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.mawen.learn.rocketmq.client.QueryResult;
import com.mawen.learn.rocketmq.client.Validators;
import com.mawen.learn.rocketmq.client.consumer.AckCallback;
import com.mawen.learn.rocketmq.client.consumer.AckResult;
import com.mawen.learn.rocketmq.client.consumer.AckStatus;
import com.mawen.learn.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.mawen.learn.rocketmq.client.consumer.MessageQueueListener;
import com.mawen.learn.rocketmq.client.consumer.MessageSelector;
import com.mawen.learn.rocketmq.client.consumer.PopCallback;
import com.mawen.learn.rocketmq.client.consumer.PopResult;
import com.mawen.learn.rocketmq.client.consumer.PopStatus;
import com.mawen.learn.rocketmq.client.consumer.PullCallback;
import com.mawen.learn.rocketmq.client.consumer.PullResult;
import com.mawen.learn.rocketmq.client.consumer.listener.MessageListener;
import com.mawen.learn.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.mawen.learn.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.mawen.learn.rocketmq.client.consumer.store.LocalFileOffsetStore;
import com.mawen.learn.rocketmq.client.consumer.store.OffsetStore;
import com.mawen.learn.rocketmq.client.consumer.store.ReadOffsetType;
import com.mawen.learn.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import com.mawen.learn.rocketmq.client.exception.MQBrokerException;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.hook.ConsumeMessageContext;
import com.mawen.learn.rocketmq.client.hook.ConsumeMessageHook;
import com.mawen.learn.rocketmq.client.hook.FilterMessageContext;
import com.mawen.learn.rocketmq.client.hook.FilterMessageHook;
import com.mawen.learn.rocketmq.client.impl.CommunicationMode;
import com.mawen.learn.rocketmq.client.impl.FindBrokerResult;
import com.mawen.learn.rocketmq.client.impl.MQClientManager;
import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.client.stat.ConsumerStatsManager;
import com.mawen.learn.rocketmq.common.KeyBuilder;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.ServiceState;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.consumer.ConsumeFromWhere;
import com.mawen.learn.rocketmq.common.help.FAQUrl;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageAccessor;
import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.common.sysflag.PullSysFlag;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.common.RemotingHelper;
import com.mawen.learn.rocketmq.remoting.exception.RemotingConnectException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingSendRequestException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTimeoutException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.mawen.learn.rocketmq.remoting.protocol.NamespaceUtil;
import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumeStatus;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.PopProcessQueueInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.ProcessQueueInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.QueueTimeSpan;
import com.mawen.learn.rocketmq.remoting.protocol.filter.FilterAPI;
import com.mawen.learn.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.MessageModel;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import com.mawen.learn.rocketmq.remoting.protocol.route.BrokerData;
import com.mawen.learn.rocketmq.remoting.protocol.route.TopicRouteData;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/23
 */
@Setter
@Getter
public class DefaultMQPushConsumerImpl implements MQConsumerInner {
	private static final Logger log = LoggerFactory.getLogger(DefaultMQPushConsumerImpl.class);

	private static final long PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL = 50;
	private static final long PULL_TIME_DELAY_MILLIS_WHEN_BROKER_FLOW_CONTROL = 20;
	private static final long PULL_TIME_DELAY_MILLIS_WHEN_SUSPEND = 1000;
	private static final long BROKER_SUSPEND_MAX_TIME_MILLIS = 15 * 1000;
	private static final long CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 30 * 1000;
	private static final int MAX_POP_INVISIBLE_TIME = 300000;
	private static final int MIN_POP_INVISIBLE_TIME = 5000;
	private static final int ASYNC_TIMEOUT = 3000;
	private static boolean doNotUpdateTopicSubscribeInfoWhenSubscriptionChange = false;

	private long pullTimeDelayMillisWhenException = 3000;

	private final DefaultMQPushConsumer defaultMQPushConsumer;
	private final RebalanceImpl rebalanceImpl = new RebalancePushImpl(this);
	private final List<FilterMessageHook> filterMessageHookList = new ArrayList<>();
	private final long consumerStartTimestamp = System.currentTimeMillis();
	private final List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<>();
	private final RPCHook rpcHook;

	private volatile ServiceState serviceState = ServiceState.CREATE_JUST;

	private MQClientInstance mqClientFactory;
	private PullAPIWrapper pullAPIWrapper;
	private volatile boolean pause = false;
	private boolean consumeOrderly = false;
	private MessageListener messageListenerInner;
	private OffsetStore offsetStore;
	private ConsumeMessageService consumeMessageService;
	private ConsumeMessageService consumeMessagePopService;
	private long queueFlowControlTimes = 0;
	private long queueMaxSpanFlowControlTimes = 0;

	private final int[] popDelayLevel = {10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200};

	public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer, RPCHook rpcHook) {
		this.defaultMQPushConsumer = defaultMQPushConsumer;
		this.rpcHook = rpcHook;
		this.pullTimeDelayMillisWhenException = defaultMQPushConsumer.getPullTimeDelayMillisWhenException();
	}

	public void registerFilterMessageHook(final FilterMessageHook hook) {
		this.filterMessageHookList.add(hook);
		log.info("register filterMessageHook Hook, {}", hook.hookName());
	}

	public boolean hasHook() {
		return !this.filterMessageHookList.isEmpty();
	}

	public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
		this.consumeMessageHookList.add(hook);
		log.info("register consumeMessageHook Hook, {}", hook.hookName());
	}

	public void executeHookBefore(final ConsumeMessageContext context) {
		if (!this.consumeMessageHookList.isEmpty()) {
			for (ConsumeMessageHook hook : this.consumeMessageHookList) {
				try {
					hook.consumeMessageBefore(context);
				}
				catch (Throwable e) {
					log.warn("consumeMessageHook {} executeHookBefore exception", hook.hookName(), e);
				}
			}
		}
	}

	public void executeHookAfter(final ConsumeMessageContext context) {
		if (!this.consumeMessageHookList.isEmpty()) {
			for (ConsumeMessageHook hook : this.consumeMessageHookList) {
				try {
					hook.consumeMessageAfter(context);
				}
				catch (Throwable e) {
					log.warn("consumeMessageHook {} executeHookAfter exception", hook.hookName(), e);
				}
			}
		}
	}

	public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
		createTopic(key, newTopic, queueNum, 0);
	}

	public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
		this.mqClientFactory.getMqAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag, null);
	}

	public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
		Set<MessageQueue> result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
		if (result == null) {
			this.mqClientFactory.updateTopicRouteInfoFromNameServer(topic);
			result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
		}

		if (result == null) {
			throw new MQClientException("The topic[" + topic + "] not exist", null);
		}

		return parseSubscribeMessageQueues(result);
	}

	public Set<MessageQueue> parseSubscribeMessageQueues(Set<MessageQueue> messageQueues) {
		return messageQueues.stream()
				.map(mq -> {
					String userTopic = NamespaceUtil.withoutNamespace(mq.getTopic(), this.defaultMQPushConsumer.getNamespace());
					return new MessageQueue(userTopic, mq.getBrokerName(), mq.getQueueId());
				})
				.collect(Collectors.toSet());
	}

	public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
		return this.mqClientFactory.getMqAdminImpl().earliestMsgStoreTime(mq);
	}

	public long maxOffset(MessageQueue mq) throws MQClientException {
		return this.mqClientFactory.getMqAdminImpl().maxOffset(mq);
	}

	public long minOffset(MessageQueue mq) throws MQClientException {
		return this.mqClientFactory.getMqAdminImpl().minOffset(mq);
	}

	public void pullMessage(final PullRequest pullRequest) {
		ProcessQueue pq = pullRequest.getProcessQueue();
		if (pq.isDropped()) {
			log.info("the pull request[{}] is dropped", pullRequest);
			return;
		}
		pullRequest.getProcessQueue().setLastPullTimestamp(System.currentTimeMillis());

		try {
			this.makeSureStateOK();
		}
		catch (MQClientException e) {
			log.warn("pullMessage exception, consumer state not OK", e);
			this.executePullRequestLater(pullRequest, pullTimeDelayMillisWhenException);
			return;
		}

		if (this.isPause()) {
			log.warn("consumer was paused, execute pull request later, instanceName={}, group={}",
					this.defaultMQPushConsumer.getInstanceName(), this.defaultMQPushConsumer.getConsumerGroup());
			this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLIS_WHEN_SUSPEND);
			return;
		}

		long cachedMessageCount = pq.getMsgCount().get();
		long cachedMessageSizeInMiB = pq.getMsgSize().get() / (1024 * 1024);
		if (cachedMessageCount > this.defaultMQPushConsumer.getPullThresholdForQueue()) {
			this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL);
			if ((queueFlowControlTimes++ % 1000) == 0) {
				log.warn("the cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
						defaultMQPushConsumer.getPullThresholdForQueue(), pq.getMsgTreeMap().firstKey(), pq.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
			}
			return;
		}

		if (cachedMessageSizeInMiB > this.defaultMQPushConsumer.getPullThresholdSizeForQueue()) {
			this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL);
			if ((queueFlowControlTimes++ % 1000) == 0) {
				log.warn("the cached message size exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
						defaultMQPushConsumer.getPullThresholdSizeForQueue(), pq.getMsgTreeMap().firstKey(), pq.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
			}
			return;
		}

		if (!this.consumeOrderly) {
			if (pq.getMaxSpan() > this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan()) {
				this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL);
				if ((queueFlowControlTimes++ % 1000) == 0) {
					log.warn("the queue's messages, span too long, so do flow control, , minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
							pq.getMsgTreeMap().firstEntry(), pq.getMsgTreeMap().lastKey(), pq.getMaxSpan(), pullRequest, queueFlowControlTimes);
				}
				return;
			}
		}
		else {
			if (pq.isLocked()) {
				if (!pullRequest.isPreviouslyLocked()) {
					long offset = -1L;
					try {
						offset = this.rebalanceImpl.computePullFromWhereWithException(pullRequest.getMessageQueue());
						if (offset < 0) {
							throw new MQClientException(ResponseCode.SYSTEM_ERROR, "Unexpected offset " + offset);
						}
					}
					catch (Exception e) {
						this.executePullRequestLater(pullRequest, pullTimeDelayMillisWhenException);
						log.error("Failed to compute pull offset, pullRequest: {}", pullRequest, e);
						return;
					}

					boolean brokerBusy = offset < pullRequest.getNextOffset();
					log.info("the first time to pull message, so fix offset from broker, pullRequest: {} NewOffset: {} brokerBusy: {}",
							pullRequest, offset, brokerBusy);
					if (brokerBusy) {
						log.info("[NOTIFY]the first tim to pull message, but pull request offset larger than broker consume offset. pullRequest: {} NewOffset: {}",
								pullRequest, offset);
					}

					pullRequest.setPreviouslyLocked(true);
					pullRequest.setNextOffset(offset);
				}
			}
			else {
				this.executePullRequestLater(pullRequest, pullTimeDelayMillisWhenException);
				log.info("pull message later because not locked in broker, {}", pullRequest);
				return;
			}
		}

		final MessageQueue mq = pullRequest.getMessageQueue();
		SubscriptionData subscriptionData = this.rebalanceImpl.getSubscriptionInner().get(mq.getTopic());
		if (subscriptionData == null) {
			this.executePullRequestLater(pullRequest, pullTimeDelayMillisWhenException);
			log.warn("find the consumer's subscription failed, {}", pullRequest);
			return;
		}

		final long beginTimestamp = System.currentTimeMillis();

		PullCallback pullCallback = new PullCallback() {
			@Override
			public void onSuccess(PullResult pullResult) {
				if (pullResult != null) {
					pullResult = DefaultMQPushConsumerImpl.this.pullAPIWrapper.processPullResult(pullRequest.getMessageQueue(), pullResult, subscriptionData);

					switch (pullResult.getPullStatus()) {
						case FOUND:
							long prevRequestOffset = pullRequest.getNextOffset();
							pullRequest.setNextOffset(pullResult.getNextBeginOffset());
							long pullRT = System.currentTimeMillis() - beginTimestamp;
							DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullRT(pullRequest.getConsumerGroup(), pullRequest.getMessageQueue().getTopic(), pullRT);

							long firstMsgOffset = Long.MAX_VALUE;
							if (pullResult.getMsgFoundList() == null || pullResult.getMsgFoundList().isEmpty()) {
								DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
							}
							else {
								firstMsgOffset = pullResult.getMsgFoundList().get(0).getQueueOffset();
								DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullRT(pullRequest.getConsumerGroup(), pullRequest.getMessageQueue().getTopic(), pullResult.getMsgFoundList().size());

								boolean dispatchToConsume = pq.putMessage(pullResult.getMsgFoundList());
								DefaultMQPushConsumerImpl.this.consumeMessageService.submitConsumeRequest(pullResult.getMsgFoundList(), pq, pullRequest.getMessageQueue(), dispatchToConsume);

								if (DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval() > 0) {
									DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval());
								}
								else {
									DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
								}
							}

							if (pullResult.getNextBeginOffset() < prevRequestOffset || firstMsgOffset < prevRequestOffset) {
								log.warn("[BUG] pull message result maybe data wrong, nextBeginOffset: {} firstMsgOffset: {} prevRequestOffset: {}",
										pullResult.getNextBeginOffset(), firstMsgOffset, prevRequestOffset);
							}

							break;
						case NO_NEW_MSG:
						case NO_MATCHED_MSG:
							pullRequest.setNextOffset(pullResult.getNextBeginOffset());

							DefaultMQPushConsumerImpl.this.correctTagsOffset(pullRequest);

							DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);

							break;
						case OFFSET_ILLEGAL:
							log.warn("the pull request offset illegal, {} {}", pullRequest, pullResult);
							pullRequest.setNextOffset(pullResult.getNextBeginOffset());
							pullRequest.getProcessQueue().setDropped(true);

							DefaultMQPushConsumerImpl.this.executeTask(() -> {
								try {
									DefaultMQPushConsumerImpl.this.offsetStore.updateAndFreezeOffset(pullRequest.getMessageQueue(), pullRequest.getNextOffset());
									DefaultMQPushConsumerImpl.this.offsetStore.persist(pullRequest.getMessageQueue());
									DefaultMQPushConsumerImpl.this.rebalanceImpl.removeProcessQueue(pullRequest.getMessageQueue());
									DefaultMQPushConsumerImpl.this.rebalanceImpl.getMqClientFactory().rebalanceImmediately();
								}
								catch (Throwable e) {
									log.error("executeTaskLater exception", e);
								}
							});
							break;
						default:
							break;
					}
				}
			}

			@Override
			public void onException(Throwable e) {
				if (!pullRequest.getMessageQueue().getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
					if (e instanceof MQBrokerException && ((MQBrokerException) e).getResponseCode() == ResponseCode.SUBSCRIPTION_NOT_LATEST) {
						log.warn("the subscription is not latest, group={}, messageQueue={}", groupName(), mq);
					}
					else {
						log.warn("execute the pull request exception, group={}, messageQueue={}", groupName(), mq);
					}
				}

				if (e instanceof MQBrokerException && ((MQBrokerException) e).getResponseCode() == ResponseCode.FLOW_CONTROL) {
					DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLIS_WHEN_BROKER_FLOW_CONTROL);
				}
				else {
					DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, pullTimeDelayMillisWhenException);
				}
			}
		};

		boolean commitOffsetEnable = false;
		long commitOffsetValue = 0L;
		if (MessageModel.CLUSTERING == this.defaultMQPushConsumer.getMessageModel()) {
			commitOffsetValue = this.offsetStore.readOffset(pullRequest.getMessageQueue(), ReadOffsetType.READ_FROM_MEMORY);
			if (commitOffsetValue > 0) {
				commitOffsetEnable = true;
			}
		}

		String subExpression = null;
		boolean classFilter = false;
		SubscriptionData sd = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
		if (sd != null) {
			if (this.defaultMQPushConsumer.isPostSubscriptionWhenPull() && !sd.isClassFilterMode()) {
				subExpression = sd.getSubString();
			}
			classFilter = sd.isClassFilterMode();
		}

		int sysFlag = PullSysFlag.buildSysFlag(commitOffsetEnable, true, subExpression != null, classFilter);

		try {
			this.pullAPIWrapper.pullKernelImpl(mq, subExpression, subscriptionData.getExpressionType(), subscriptionData.getSubVersion(), pullRequest.getNextOffset(),
					this.defaultMQPushConsumer.getPullBatchSize(), this.defaultMQPushConsumer.getPullBatchSizeInBytes(),
					sysFlag, commitOffsetValue, BROKER_SUSPEND_MAX_TIME_MILLIS, CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND, CommunicationMode.ASYNC, pullCallback);
		}
		catch (Exception e) {
			log.error("pullKernelImpl exception", e);
			this.executePullRequestLater(pullRequest, pullTimeDelayMillisWhenException);
		}
	}

	public MessageExt viewMessage(String topic, String msgId) throws MQClientException {
		return this.mqClientFactory.getMqAdminImpl().viewMessage(topic, msgId);
	}

	public void resetOffsetByTimestamp(long timestamp) {
		for (String topic : rebalanceImpl.getSubscriptionInner().keySet()) {
			Set<MessageQueue> mqs = rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
			if (CollectionUtils.isNotEmpty(mqs)) {
				Map<MessageQueue, Long> offsetTable = new HashMap<>(mqs.size(), 1);
				for (MessageQueue mq : mqs) {
					long offset = searchOffset(mq, timestamp);
					offsetTable.put(mq, offset);
				}
				this.mqClientFactory.resetOffset(topic, groupName(), offsetTable);
			}
		}
	}

	public long searchOffset(MessageQueue mq, long timestamp) {
		return this.mqClientFactory.getMqAdminImpl().searchOffset(mq, timestamp);
	}

	public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end) throws InterruptedException, MQClientException {
		return this.mqClientFactory.getMqAdminImpl().queryMessage(topic, key, maxNum, begin, end);
	}

	public MessageExt queryMessageByUniqKey(String topic, String uniqKey) throws InterruptedException, MQClientException {
		return this.mqClientFactory.getMqAdminImpl().queryMessageByUniqKey(topic, uniqKey);
	}

	public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
		sendMessageBack(msg, delayLevel, brokerName, null);
	}

	public void sendMessageBack(MessageExt msg, int delayLevel, final MessageQueue mq) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
		sendMessageBack(msg, delayLevel, null, mq);
	}

	@Override
	public String groupName() {
		return this.defaultMQPushConsumer.getConsumerGroup();
	}

	@Override
	public MessageModel messageModel() {
		return this.defaultMQPushConsumer.getMessageModel();
	}

	@Override
	public ConsumeType consumeType() {
		return ConsumeType.CONSUME_PASSIVELY;
	}

	@Override
	public ConsumeFromWhere consumeFromWhere() {
		return this.defaultMQPushConsumer.getConsumeFromWhere();
	}

	@Override
	public Set<SubscriptionData> subscriptions() {
		return new HashSet<>(this.rebalanceImpl.getSubscriptionInner().values());
	}

	@Override
	public void doRebalance() {
		if (!this.pause) {
			this.rebalanceImpl.doRebalance(this.isConsumeOrderly());
		}
	}

	@Override
	public boolean tryBalance() {
		if (!this.pause) {
			return this.rebalanceImpl.doRebalance(this.isConsumeOrderly());
		}
		return false;
	}

	@Override
	public void persistConsumerOffset() {
		try {
			this.makeSureStateOK();
			Set<MessageQueue> mqs = new HashSet<>();
			Set<MessageQueue> allocateMQ = this.rebalanceImpl.getProcessQueueTable().keySet();
			mqs.addAll(allocateMQ);

			this.offsetStore.persistAll(mqs);
		}
		catch (Exception e) {
			log.error("group: {} persistConsumerOffset exception", this.defaultMQPushConsumer.getConsumerGroup(), e);
		}
	}

	@Override
	public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
		ConcurrentMap<String, SubscriptionData> subTable = this.getSubscriptionInner();
		if (subTable != null && subTable.containsKey(topic)) {
			this.rebalanceImpl.topicSubscribeInfoTable.put(topic, info);
		}
	}

	@Override
	public boolean isSubscribeTopicNeedUpdate(String topic) {
		ConcurrentMap<String, SubscriptionData> subTable = this.getSubscriptionInner();
		if (subTable != null && subTable.containsKey(topic)) {
			return !this.rebalanceImpl.topicSubscribeInfoTable.containsKey(topic);
		}
		return false;
	}

	@Override
	public boolean isUnitMode() {
		return this.defaultMQPushConsumer.isUnitMode();
	}

	@Override
	public ConsumerRunningInfo consumerRunningInfo() {
		Properties prop = MixAll.object2Properties(this.defaultMQPushConsumer);
		prop.put(ConsumerRunningInfo.PROP_CONSUME_ORDERLY, String.valueOf(this.consumeOrderly));
		prop.put(ConsumerRunningInfo.PROP_THREADPOOL_CORE_SIZE, String.valueOf(this.consumeMessageService.getCorePoolSize()));
		prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, String.valueOf(this.consumerStartTimestamp));

		ConsumerRunningInfo info = new ConsumerRunningInfo();

		info.setProperties(prop);

		Set<SubscriptionData> subSet = this.subscriptions();
		info.getSubscriptionSet().addAll(subSet);

		for (Map.Entry<MessageQueue, ProcessQueue> entry : this.rebalanceImpl.getProcessQueueTable().entrySet()) {
			MessageQueue mq = entry.getKey();
			ProcessQueue pq = entry.getValue();

			ProcessQueueInfo processQueueInfo = new ProcessQueueInfo();
			processQueueInfo.setCommitOffset(this.offsetStore.readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE));
			pq.fillProcessQueueInfo(processQueueInfo);
			info.getMqTable().put(mq, processQueueInfo);
		}

		for (Map.Entry<MessageQueue, PopProcessQueue> entry : this.rebalanceImpl.getPopProcessQueueTable().entrySet()) {
			MessageQueue mq = entry.getKey();
			PopProcessQueue pq = entry.getValue();

			PopProcessQueueInfo popProcessQueueInfo = new PopProcessQueueInfo();
			pq.fillPopProcessQueueInfo(popProcessQueueInfo);
			info.getMqPopTable().put(mq, popProcessQueueInfo);
		}

		for (SubscriptionData sd : subSet) {
			ConsumeStatus consumeStatus = this.mqClientFactory.getConsumerStatsManager().consumeStatus(groupName(), sd.getTopic());
			info.getStatusTable().put(sd.getTopic(), consumeStatus);
		}

		return info;
	}

	public void executeTaskLater(final Runnable r, final long timeDelay) {
		this.mqClientFactory.getPullMessageService().executeTaskLater(r, timeDelay);
	}

	public void executeTask(final Runnable r) {
		this.mqClientFactory.getPullMessageService().executeTask(r);
	}

	public void registerMessageListener(MessageListener messageListener) {
		this.messageListenerInner = messageListener;
	}

	public int getMaxReconsumeTimes() {
		if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
			return 16;
		}
		return this.defaultMQPushConsumer.getMaxReconsumeTimes();
	}

	public synchronized void start() throws MQClientException {
		switch (this.serviceState) {
			case CREATE_JUST:
				log.info("the consumer [{}] start begining, messageModel={}, isUnitMode={}", groupName(), messageModel(), isUnitMode());
				this.serviceState = ServiceState.START_FAILED;

				this.checkConfig();

				this.copySubscription();

				if (messageModel() == MessageModel.CLUSTERING) {
					this.defaultMQPushConsumer.changeInstanceNameToPID();
				}

				this.mqClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQPushConsumer, this.rpcHook);

				this.rebalanceImpl.setConsumerGroup(groupName());
				this.rebalanceImpl.setMessageModel(messageModel());
				this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultMQPushConsumer.getAllocateMessageQueueStrategy());
				this.rebalanceImpl.setMqClientFactory(this.mqClientFactory);

				if (this.pullAPIWrapper == null) {
					this.pullAPIWrapper = new PullAPIWrapper(this.mqClientFactory, groupName(), isUnitMode());
				}
				this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);

				if (this.defaultMQPushConsumer.getOffsetStore() != null) {
					this.offsetStore = this.defaultMQPushConsumer.getOffsetStore();
				}
				else {
					switch (messageModel()) {
						case BROADCASTING:
							this.offsetStore = new LocalFileOffsetStore(this.mqClientFactory, groupName());
							break;
						case CLUSTERING:
							this.offsetStore = new RemoteBrokerOffsetStore(this.mqClientFactory, groupName());
							break;
						default:
							break;
					}
					this.defaultMQPushConsumer.setOffsetStore(this.offsetStore);
				}
				this.offsetStore.load();

				if (this.getMessageListenerInner() instanceof MessageListenerOrderly) {
					this.consumeOrderly = true;
					this.consumeMessageService = new ConsumeMessageOrderlyService(this, (MessageListenerOrderly) this.getMessageQueueListener());
					this.consumeMessagePopService = new ConsumeMessagePopOrderlyService(this, (MessageListenerOrderly) this.getMessageQueueListener());
				}
				else if (this.getMessageListenerInner() instanceof MessageListenerConcurrently) {
					this.consumeOrderly = false;
					this.consumeMessageService = new ConsumeMessageConcurrentlyService(this, (MessageListenerConcurrently) this.getMessageListenerInner());
					this.consumeMessagePopService = new ConsumeMessagePopConcurrentlyService(this, (MessageListenerConcurrently) this.getMessageListenerInner());
				}

				this.consumeMessageService.start();
				this.consumeMessagePopService.start();

				boolean registerOk = mqClientFactory.registerConsumer(groupName(), this);
				if (!registerOk) {
					this.serviceState = ServiceState.CREATE_JUST;
					this.consumeMessageService.shutdown(defaultMQPushConsumer.getAwaitTerminationMillisWhenShutdown());
					throw new MQClientException("The consumer group[" + groupName() + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL), null);
				}

				mqClientFactory.start();
				log.info("the consumer [{}] start OK.", groupName());
				this.serviceState = ServiceState.RUNNING;
				break;
			case RUNNING:
			case START_FAILED:
			case SHUTDOWN_ALREADY:
				throw new MQClientException("The PushConsumer service state not OK, maybe started once, " + this.serviceState + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
			default:
				break;
		}

		this.updateTopicSubscribeInfoWhenSubscriptionChanged();
		this.mqClientFactory.checkClientInBroker();
		if (this.mqClientFactory.sendHeartbeatToAllBrokerWithLock()) {
			this.mqClientFactory.rebalanceImmediately();
		}
	}

	public void shutdown() {
		shutdown(0);
	}

	public synchronized void shutdown(long awaitTerminateMillis) {
		switch (this.serviceState) {
			case CREATE_JUST:
				break;
			case RUNNING:
				this.consumeMessageService.shutdown(awaitTerminateMillis);
				this.persistConsumerOffset();
				this.mqClientFactory.unregisterConsumer(this.defaultMQPushConsumer.getConsumerGroup());
				this.mqClientFactory.shutdown();

				log.info("the consumer [{}] shutdown OK", groupName());

				this.rebalanceImpl.destroy();
				this.serviceState = ServiceState.SHUTDOWN_ALREADY;
				break;
			case SHUTDOWN_ALREADY:
				break;
			default:
				break;
		}
	}

	public void resume() {
		this.pause = false;
		doRebalance();
		log.info("resume the consumer, {}", groupName());
	}

	public void subscribe(String topic, String subExpression) throws MQClientException {
		try {
			SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subExpression);
			this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
			if (this.mqClientFactory != null) {
				this.mqClientFactory.sendHeartbeatToAllBrokerWithLock();
			}
		}
		catch (Exception e) {
			throw new MQClientException("subscription exception", e);
		}
	}

	public void subscribe(String topic, String fullClassName, String filterClassSource) throws MQClientException {
		try {
			SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, SubscriptionData.SUB_ALL);
			subscriptionData.setSubString(fullClassName);
			subscriptionData.setClassFilterMode(true);
			subscriptionData.setFilterClassSource(filterClassSource);

			this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
			if (this.mqClientFactory != null) {
				this.mqClientFactory.sendHeartbeatToAllBrokerWithLock();
			}
		}
		catch (Exception e) {
			throw new MQClientException("subscription exception", e);
		}
	}

	public void subscribe(final String topic, final MessageSelector messageSelector) throws MQClientException {
		try {
			if (messageSelector == null) {
				subscribe(topic, SubscriptionData.SUB_ALL);
				return;
			}

			SubscriptionData subscriptionData = FilterAPI.build(topic, messageSelector.getExpression(), messageSelector.getType());
			this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
			if (this.mqClientFactory != null) {
				this.mqClientFactory.sendHeartbeatToAllBrokerWithLock();
			}
		}
		catch (Exception e) {
			throw new MQClientException("subscription exception", e);
		}
	}

	public void suspend() {
		this.pause = true;
		log.info("suspend this consumer, {}", groupName());
	}

	public void unsubscribe(String topic) {
		this.rebalanceImpl.getSubscriptionInner().remove(topic);
	}

	public void updateConsumeOffset(MessageQueue mq, long offset) {
		this.offsetStore.updateOffset(mq, offset, false);
	}

	public void updateCorePoolSize(int corePoolSize) {
		this.consumeMessageService.updateCorePoolSize(corePoolSize);
	}

	public void adjustThreadPool() {
		long computeAccTotal = this.computeAccumulationTotal();
		long adjustThreadPoolNumsThread = this.defaultMQPushConsumer.getAdjustThreadPoolNumsThread();

		long incThreshold = (long) (adjustThreadPoolNumsThread * 1.0);
		long decThreshold = (long) (adjustThreadPoolNumsThread * 0.8);

		if (computeAccTotal >= incThreshold) {
			this.consumeMessageService.incCorePoolSize();
		}
		if (computeAccTotal < decThreshold) {
			this.consumeMessageService.decCorePoolSize();
		}
	}

	public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
		return this.rebalanceImpl.getSubscriptionInner();
	}

	public ConsumerStatsManager getConsumerStatsManager() {
		return this.mqClientFactory.getConsumerStatsManager();
	}

	public List<QueueTimeSpan> queryConsumeTimeSpan(final String topic) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
		List<QueueTimeSpan> queueTimeSpans = new ArrayList<>();
		TopicRouteData routeData = this.mqClientFactory.getMqClientAPIImpl().getTopicRouteInfoFromNameServer(topic, 3000);
		for (BrokerData brokerData : routeData.getBrokerDatas()) {
			String addr = brokerData.selectBrokerAddr();
			queueTimeSpans.addAll(this.mqClientFactory.getMqClientAPIImpl().queryConsumeTimeSpan(addr, topic, groupName(), 3000));
		}
		return queueTimeSpans;
	}

	public void tryResetRetryTopic(final List<MessageExt> msgs, String consumerGroup) {
		String popRetryPrefix = MixAll.RETRY_GROUP_TOPIC_PREFIX + consumerGroup + "_";
		msgs.stream()
				.filter(msg -> msg.getTopic().startsWith(popRetryPrefix))
				.forEach(msg -> {
					String normalTopic = KeyBuilder.parseNormalTopic(msg.getTopic(), consumerGroup);
					if (StringUtils.isNotEmpty(normalTopic)) {
						msg.setTopic(normalTopic);
					}
				});
	}

	public void resetRetryAndNamespace(final List<MessageExt> msgs, String consumerGroup) {
		String groupTopic = MixAll.getRetryTopic(consumerGroup);
		for (MessageExt msg : msgs) {
			String retryTopic = msg.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
			if (retryTopic != null && groupTopic.equals(msg.getTopic())) {
				msg.setTopic(retryTopic);
			}

			if (StringUtils.isNotEmpty(this.defaultMQPushConsumer.getNamespace())) {
				msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));

			}
		}
	}

	public MessageQueueListener getMessageQueueListener() {
		if (this.defaultMQPushConsumer == null) {
			return null;
		}
		return this.defaultMQPushConsumer.getMessageQueueListener();
	}

	public void executePullRequestImmediately(final PullRequest pullRequest) {
		this.mqClientFactory.getPullMessageService().executePullRequestImmediately(pullRequest);
	}

	void popMessage(final PopRequest popRequest) {
		PopProcessQueue pq = popRequest.getPopProcessQueue();
		if (pq.isDropped()) {
			log.info("the pop request[{}] is dropped.", popRequest);
			return;
		}

		pq.setLastPopTimestamp(System.currentTimeMillis());

		try {
			this.makeSureStateOK();
		}
		catch (MQClientException e) {
			log.warn("pullMessage exception, consumer state not ok", e);
			this.executePopPullRequestLater(popRequest, pullTimeDelayMillisWhenException);
		}

		if (this.isPause()) {
			log.warn("consumer was paused, execte pull request later. instanceName={}, group={}", this.defaultMQPushConsumer.getInstanceName(), groupName());
			this.executePopPullRequestLater(popRequest, PULL_TIME_DELAY_MILLIS_WHEN_SUSPEND);
			return;
		}

		if (pq.getWaitAckMsgCount() > this.defaultMQPushConsumer.getPopThresholdForQueue()) {
			this.executePopPullRequestLater(popRequest, PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL);
			if ((queueFlowControlTimes++ % 1000) == 0) {
				log.warn("the messages waiting to ack exceeds the threshold {}, so do flow control, popRequest={}, flowControlTimes={}, wait count={}",
						this.defaultMQPushConsumer.getPopThresholdForQueue(), popRequest, queueFlowControlTimes, pq.getWaitAckMsgCount());
			}
			return;
		}

		SubscriptionData subscriptionData = this.rebalanceImpl.getSubscriptionInner().get(popRequest.getMessageQueue().getTopic());
		if (subscriptionData == null) {
			this.executePopPullRequestLater(popRequest, pullTimeDelayMillisWhenException);
			log.warn("find the consumer's subscription failed, {}", popRequest);
			return;
		}

		final long begin = System.currentTimeMillis();
		PopCallback popCallback = new PopCallback() {
			@Override
			public void onSuccess(PopResult popResult) {
				if (popRequest == null) {
					log.error("pop callback popResult is null");
					DefaultMQPushConsumerImpl.this.executePopPullRequestImmediately(popRequest);
					return;
				}

				processPopResult(popResult, subscriptionData);

				switch (popResult.getPopStatus()) {
					case FOUND:
						long pullRT = System.currentTimeMillis() - begin;
						String consumerGroup = popRequest.getConsumerGroup();
						String topic = popRequest.getMessageQueue().getTopic();

						DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullRT(consumerGroup, topic, pullRT);
						if (CollectionUtils.isEmpty(popResult.getMsgFoundList())) {
							DefaultMQPushConsumerImpl.this.executePopPullRequestImmediately(popRequest);
						}
						else {
							int foundMsgSize = popResult.getMsgFoundList().size();
							DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullTPS(consumerGroup, topic, foundMsgSize);

							popRequest.getPopProcessQueue().incFoundMsg(foundMsgSize);

							DefaultMQPushConsumerImpl.this.consumeMessagePopService.submitPopConsumeRequest(popResult.getMsgFoundList(), pq, popRequest.getMessageQueue());

							if (DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval() > 0) {
								DefaultMQPushConsumerImpl.this.executePopPullRequestLater(popRequest, DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval());
							}
							else {
								DefaultMQPushConsumerImpl.this.executePopPullRequestImmediately(popRequest);
							}
						}
						break;
					case NO_NEW_MSG:
					case POLLING_NOT_FOUND:
						DefaultMQPushConsumerImpl.this.executePopPullRequestImmediately(popRequest);
						break;
					case POLLING_FULL:
						DefaultMQPushConsumerImpl.this.executePopPullRequestLater(popRequest, pullTimeDelayMillisWhenException);
						break;
					default:
						DefaultMQPushConsumerImpl.this.executePopPullRequestLater(popRequest, pullTimeDelayMillisWhenException);
						break;
				}
			}

			@Override
			public void onException(Throwable e) {
				if (!popRequest.getMessageQueue().getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
					log.warn("execute the pull request exception: {}", e);
				}

				if (e instanceof MQBrokerException && ((MQBrokerException) e).getResponseCode() == ResponseCode.FLOW_CONTROL) {
					DefaultMQPushConsumerImpl.this.executePopPullRequestLater(popRequest, PULL_TIME_DELAY_MILLIS_WHEN_BROKER_FLOW_CONTROL);
				}
				else {
					DefaultMQPushConsumerImpl.this.executePopPullRequestLater(popRequest, pullTimeDelayMillisWhenException);
				}
			}
		};

		try {
			long invisibleTime = this.defaultMQPushConsumer.getPopInvisibleTime();
			if (invisibleTime < MIN_POP_INVISIBLE_TIME || invisibleTime > MAX_POP_INVISIBLE_TIME) {
				invisibleTime = 60000;
			}

			this.pullAPIWrapper.popAsync(popRequest.getMessageQueue(), invisibleTime, this.defaultMQPushConsumer.getPopBatchNums(),
					popRequest.getConsumerGroup(), BROKER_SUSPEND_MAX_TIME_MILLIS, popCallback, true, popRequest.getInitMode(),
					false, subscriptionData.getExpressionType(), subscriptionData.getSubString());
		}
		catch (Exception e) {
			log.error("popAsync exception", e);
			this.executePopPullRequestLater(popRequest, pullTimeDelayMillisWhenException);
		}
	}

	void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
		this.mqClientFactory.getPullMessageService().executePullRequestLater(pullRequest, timeDelay);
	}

	void executePopPullRequestLater(final PopRequest popRequest, final long timeDelay) {
		this.mqClientFactory.getPullMessageService().executePopPullRequestLater(popRequest, timeDelay);
	}

	void executePopPullRequestImmediately(final PopRequest popRequest) {
		this.mqClientFactory.getPullMessageService().executePopPullRequestImmediately(popRequest);
	}

	void ackAsync(MessageExt msg, String consumerGroup) {
		String extraInfo = msg.getProperty(MessageConst.PROPERTY_POP_CK);

		try {
			String[] arr = ExtraInfoUtil.split(extraInfo);
			String brokerName = ExtraInfoUtil.getBrokerName(arr);
			int queueId = ExtraInfoUtil.getQueueId(arr);
			long queueOffset = ExtraInfoUtil.getQueueOffset(arr);
			String topic = msg.getTopic();

			String destBrokerName = brokerName;
			if (brokerName != null && brokerName.startsWith(MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX)) {
				destBrokerName = this.mqClientFactory.getBrokerNameFromMessageQueue(this.defaultMQPushConsumer.queueWithNamespace(new MessageQueue(topic, brokerName, queueId)));
			}

			FindBrokerResult result = this.mqClientFactory.findBrokerAddressInSubscribe(destBrokerName, MixAll.MASTER_ID, true);
			if (result == null) {
				this.mqClientFactory.updateTopicRouteInfoFromNameServer(topic);
				result = this.mqClientFactory.findBrokerAddressInSubscribe(destBrokerName, MixAll.MASTER_ID, true);
			}

			if (result == null) {
				log.error("The broker[" + destBrokerName + "] not eixst");
				return;
			}

			AckMessageRequestHeader requestHeader = new AckMessageRequestHeader();
			requestHeader.setTopic(ExtraInfoUtil.getRealTopic(arr, topic, consumerGroup));
			requestHeader.setQueueId(queueId);
			requestHeader.setOffset(queueOffset);
			requestHeader.setConsumerGroup(consumerGroup);
			requestHeader.setExtraInfo(extraInfo);
			requestHeader.setBrokerName(brokerName);

			this.mqClientFactory.getMqClientAPIImpl().ackMessageAsync(result.getBrokerAddr(), ASYNC_TIMEOUT, new AckCallback() {
				@Override
				public void onSuccess(AckResult ackResult) {
					if (ackResult != null && !AckStatus.OK.equals(ackResult.getStatus())) {
						log.warn("Ack message fail. ackResult: {}, extraInfo: {}", ackResult, extraInfo);
					}
				}

				@Override
				public void onException(Throwable e) {
					log.warn("Ack message fail. extraInfo: {}, error message: {}", extraInfo, e);
				}
			}, requestHeader);
		}
		catch (Throwable t) {
			log.error("ack async error", t);
		}
	}

	void changePopInvisibleTimeAsync(String topic, String consumerGroup, String extraInfo, long invisibleTime, AckCallback ackCallback) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingTooMuchRequestException, MQClientException {
		String[] strs = ExtraInfoUtil.split(extraInfo);
		String brokerName = ExtraInfoUtil.getBrokerName(strs);
		int queueId = ExtraInfoUtil.getQueueId(strs);

		String destBrokerName = brokerName;
		if (brokerName != null && brokerName.startsWith(MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX)) {
			destBrokerName = this.mqClientFactory.getBrokerNameFromMessageQueue(this.defaultMQPushConsumer.queueWithNamespace(new MessageQueue(topic, brokerName, queueId)));
		}

		FindBrokerResult result = this.mqClientFactory.findBrokerAddressInSubscribe(destBrokerName, MixAll.MASTER_ID, true);
		if (result == null) {
			this.mqClientFactory.updateTopicRouteInfoFromNameServer(topic);
			result = this.mqClientFactory.findBrokerAddressInSubscribe(destBrokerName, MixAll.MASTER_ID, true);
		}

		if (result != null) {
			ChangeInvisibleTimeRequestHeader requestHeader = new ChangeInvisibleTimeRequestHeader();
			requestHeader.setTopic(ExtraInfoUtil.getRealTopic(strs, topic, consumerGroup));
			requestHeader.setQueueId(queueId);
			requestHeader.setOffset(ExtraInfoUtil.getQueueOffset(strs));
			requestHeader.setConsumerGroup(consumerGroup);
			requestHeader.setExtraInfo(extraInfo);
			requestHeader.setInvisibleTime(invisibleTime);
			requestHeader.setBrokerName(brokerName);

			this.mqClientFactory.getMqClientAPIImpl().changeInvisibleTimeAsync(brokerName, result.getBrokerAddr(), requestHeader, ASYNC_TIMEOUT, ackCallback);
			return;
		}

		throw new MQClientException("The broker[" + destBrokerName + "] not exist", null);
	}

	private PopResult processPopResult(final PopResult popResult, final SubscriptionData subscriptionData) {
		if (popResult.getPopStatus() == PopStatus.FOUND) {
			List<MessageExt> msgFoundList = popResult.getMsgFoundList();
			List<MessageExt> msgListFilterAgain = msgFoundList;
			if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode() && popResult.getMsgFoundList().size() > 0) {
				msgListFilterAgain = popResult.getMsgFoundList()
						.stream()
						.filter(msg -> msg.getTags() != null)
						.filter(msg -> subscriptionData.getTagsSet().contains(msg.getTags()))
						.collect(Collectors.toList());
			}

			if (!this.filterMessageHookList.isEmpty()) {
				FilterMessageContext context = new FilterMessageContext();
				context.setUnitMode(isUnitMode());
				context.setMsgList(msgListFilterAgain);
				if (!this.filterMessageHookList.isEmpty()) {
					for (FilterMessageHook hook : this.filterMessageHookList) {
						try {
							hook.filterMessage(context);
						}
						catch (Throwable e) {
							log.error("execute hook error. hookName={}", hook.hookName());
						}
					}
				}
			}

			if (msgFoundList.size() != msgListFilterAgain.size()) {
				for (MessageExt msg : msgFoundList) {
					if (!msgListFilterAgain.contains(msg)) {
						ackAsync(msg, groupName());
					}
				}
			}

			popResult.setMsgFoundList(msgListFilterAgain);
		}

		return popResult;
	}

	private void correctTagsOffset(final PullRequest pullRequest) {
		if (pullRequest.getProcessQueue().getMsgCount().get() == 0L) {
			this.offsetStore.updateOffset(pullRequest.getMessageQueue(), pullRequest.getNextOffset(), true);
		}
	}

	private void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName, final MessageQueue mq) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
		boolean needRetry = true;
		try {
			if (brokerName != null && brokerName.startsWith(MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX) || mq != null && mq.getBrokerName().startsWith(MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX)) {
				needRetry = false;
				sendMessageBackNormalMessage(msg);
			}
			else {
				String brokerAddr = brokerName != null ? this.mqClientFactory.findBrokerAddressInPublish(brokerName)
						: RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());
				this.mqClientFactory.getMqClientAPIImpl().consumeSendMessageBack(brokerAddr, brokerName, msg, groupName(), delayLevel, 5000, getMaxReconsumeTimes());
			}
		}
		catch (Throwable t) {
			log.error("Failed to send message back, consumerGroup={}, brokerName={}, mq={}, message={}",
					groupName(), brokerName, mq, msg, t);
			if (needRetry) {
				sendMessageBackNormalMessage(msg);
			}
		}
		finally {
			msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
		}
	}

	private void sendMessageBackNormalMessage(MessageExt msg) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
		Message newMsg = new Message(MixAll.getRetryTopic(groupName()), msg.getBody());

		String originMessageId = MessageAccessor.getOriginMessageId(msg);
		MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMessageId) ? msg.getMsgId() : originMessageId);

		newMsg.setFlag(msg.getFlag());
		MessageAccessor.setProperties(newMsg, msg.getProperties());
		MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
		MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes() + 1));
		MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
		MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);
		newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

		this.mqClientFactory.getDefaultMQProducer().send(newMsg);
	}

	private void checkConfig() throws MQClientException {
		Validators.checkGroup(groupName());

		if (groupName().equals(MixAll.DEFAULT_CONSUMER_GROUP)) {
			throw new MQClientException("consumerGroup can not equal to " + MixAll.DEFAULT_CONSUMER_GROUP
					+ ", please specify another one. " + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
		}

		if (this.defaultMQPushConsumer.getMessageModel() == null) {
			throw new MQClientException("messageModel is null " + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
		}

		if (this.defaultMQPushConsumer.getConsumeFromWhere() == null) {
			throw new MQClientException("consumeFromWhere is null " + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
		}

		Date date = UtilAll.parseDate(this.defaultMQPushConsumer.getConsumeTimestamp(), UtilAll.YYYYMMDDHHMMSS);
		if (date == null) {
			throw new MQClientException("consumeTimestamp is invalid, the valid format is yyyyMMDDHHMMss, but received " + this.defaultMQPushConsumer.getConsumeTimestamp() + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
		}

		if (this.defaultMQPushConsumer.getAllocateMessageQueueStrategy() == null) {
			throw new MQClientException("allocateMessageQueueStrategy is null " + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
		}

		if (this.defaultMQPushConsumer.getSubscription() == null) {
			throw new MQClientException("subscription is null " + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
		}

		if (this.defaultMQPushConsumer.getMessageListener() == null) {
			throw new MQClientException("messageListener is null " + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
		}

		boolean orderly = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerOrderly;
		boolean concurrently = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerConcurrently;
		if (!orderly && !concurrently) {
			throw new MQClientException("messageListener must be instanceof MessageListenerOrderly or MessageListenerConcurrently" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
		}

		if (this.defaultMQPushConsumer.getConsumeThreadMin() < 1 || this.defaultMQPushConsumer.getConsumeThreadMin() > 1000) {
			throw new MQClientException("consumerThreadMin Out of range [1, 1000]" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
		}

		if (this.defaultMQPushConsumer.getConsumeThreadMax() < 1 || this.defaultMQPushConsumer.getConsumeThreadMax() > 1000) {
			throw new MQClientException("consumerThreadMax Out of range [1, 1000]" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
		}

		if (this.defaultMQPushConsumer.getConsumeThreadMin() > this.defaultMQPushConsumer.getConsumeThreadMax()) {
			throw new MQClientException("consumeThreadMin (" + this.defaultMQPushConsumer.getConsumeThreadMin() + ") is larger than consumeThreadMax (" + this.defaultMQPushConsumer.getConsumeThreadMax() + ")", null);
		}

		if (this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() < 1 || this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() > 65535) {
			throw new MQClientException("consumeConcurrentlyMaxSpan Out of range [1, 65535]" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
		}

		if (this.defaultMQPushConsumer.getPullThresholdForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdForQueue() > 65535) {
			throw new MQClientException("pullThresholdForQueue Out of range [1, 65535]" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
		}

		if (this.defaultMQPushConsumer.getPullThresholdForTopic() != -1) {
			if (this.defaultMQPushConsumer.getPullThresholdForTopic() < 1 || this.defaultMQPushConsumer.getPullThresholdForTopic() > 6553500) {
				throw new MQClientException("pullThresholdForTopic Out of range [1, 6553500]" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
			}
		}

		if (this.defaultMQPushConsumer.getPullThresholdSizeForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdSizeForQueue() > 1024) {
			throw new MQClientException("pullThresholdizeForQueue Out of range [1, 102400]" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
		}

		if (this.defaultMQPushConsumer.getPullInterval() < 0 || this.defaultMQPushConsumer.getPullInterval() > 65535) {
			throw new MQClientException("pullInterval Out of range [0, 65535]" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
		}

		if (this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() < 1 || this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() > 1024) {
			throw new MQClientException("consumeMessageBatchMaxSize Out of range [1, 1024]" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
		}

		if (this.defaultMQPushConsumer.getPullBatchSize() < 1 || this.defaultMQPushConsumer.getPullBatchSize() > 1024) {
			throw new MQClientException("pullBatchSize Out of range [1, 1024]" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
		}

		if (this.defaultMQPushConsumer.getPopInvisibleTime() < MIN_POP_INVISIBLE_TIME || this.defaultMQPushConsumer.getPopInvisibleTime() > MAX_POP_INVISIBLE_TIME) {
			throw new MQClientException("popInvisibleTime Out of range [" + MIN_POP_INVISIBLE_TIME+ ", " + MAX_POP_INVISIBLE_TIME + "]" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
		}

		if (this.defaultMQPushConsumer.getPopBatchNums() <= 0 || this.defaultMQPushConsumer.getPopBatchNums() > 32) {
			throw new MQClientException("popBatchNums Out of range [1, 32]" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
		}
	}

	private void copySubscription() throws MQClientException {
		try {
			Map<String, String> sub = this.defaultMQPushConsumer.getSubscription();
			if (sub != null) {
				for (Map.Entry<String, String> entry : sub.entrySet()) {
					String topic = entry.getKey();
					String subString = entry.getValue();
					SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subString);
					this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
				}
			}

			if (this.messageListenerInner == null) {
				this.messageListenerInner = this.defaultMQPushConsumer.getMessageListener();
			}

			switch (this.defaultMQPushConsumer.getMessageModel()) {
				case BROADCASTING:
					break;
				case CLUSTERING:
					String retryTopic = MixAll.getRetryTopic(groupName());
					SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(retryTopic, SubscriptionData.SUB_ALL);
					this.rebalanceImpl.getSubscriptionInner().put(retryTopic, subscriptionData);
					break;
				default:
					break;
			}
		}
		catch (Exception e) {
			throw new MQClientException("subscription exception", e);
		}
	}

	private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
		if (doNotUpdateTopicSubscribeInfoWhenSubscriptionChange) {
			return;
		}

		ConcurrentMap<String, SubscriptionData> subTable = this.getSubscriptionInner();
		if (subTable != null) {
			for (String topic : subTable.keySet()) {
				this.mqClientFactory.updateTopicRouteInfoFromNameServer(topic);
			}
		}
	}

	private void makeSureStateOK() throws MQClientException {
		if (this.serviceState != ServiceState.RUNNING) {
			throw new MQClientException("The consumer service state not OK, " + this.serviceState + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK), null);
		}
	}

	private long computeAccumulationTotal() {
		ConcurrentMap<MessageQueue, ProcessQueue> queueMap = this.rebalanceImpl.getProcessQueueTable();
		return queueMap.values().stream().mapToLong(ProcessQueue::getMsgAccCnt).sum();
	}
}
