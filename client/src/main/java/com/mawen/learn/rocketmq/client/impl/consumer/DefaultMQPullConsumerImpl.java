package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import javax.swing.*;

import com.mawen.learn.rocketmq.client.QueryResult;
import com.mawen.learn.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.mawen.learn.rocketmq.client.consumer.MessageSelector;
import com.mawen.learn.rocketmq.client.consumer.PullCallback;
import com.mawen.learn.rocketmq.client.consumer.PullResult;
import com.mawen.learn.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.mawen.learn.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.mawen.learn.rocketmq.client.consumer.store.OffsetStore;
import com.mawen.learn.rocketmq.client.consumer.store.ReadOffsetType;
import com.mawen.learn.rocketmq.client.exception.MQBrokerException;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.hook.ConsumeMessageContext;
import com.mawen.learn.rocketmq.client.hook.ConsumeMessageHook;
import com.mawen.learn.rocketmq.client.hook.FilterMessageHook;
import com.mawen.learn.rocketmq.client.impl.CommunicationMode;
import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.ServiceState;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.consumer.ConsumeFromWhere;
import com.mawen.learn.rocketmq.common.filter.ExpressionType;
import com.mawen.learn.rocketmq.common.help.FAQUrl;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageAccessor;
import com.mawen.learn.rocketmq.common.message.MessageConst;
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
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import com.mawen.learn.rocketmq.remoting.protocol.filter.FilterAPI;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.MessageModel;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import com.sun.org.apache.xml.internal.utils.NameSpace;
import javassist.bytecode.analysis.Util;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/29
 */
@Getter
@Setter
public class DefaultMQPullConsumerImpl implements MQConsumerInner {
	private static final Logger log = LoggerFactory.getLogger(DefaultMQPushConsumerImpl.class);

	private final DefaultMQPullConsumer defaultMQPullConsumer;
	private final long consumerStartTimestamp = System.currentTimeMillis();
	private final RPCHook rpcHook;
	private final List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<>();
	private final List<FilterMessageHook> filterMessageHookList = new ArrayList<>();
	private volatile ServiceState serviceState = ServiceState.CREATE_JUST;
	private MQClientInstance mqClientFactory;
	private PullAPIWrapper pullAPIWrapper;
	private OffsetStore offsetStore;
	private RebalanceImpl rebalanceImpl = new RebalancePullImpl(this);

	public DefaultMQPullConsumerImpl(DefaultMQPullConsumer defaultMQPullConsumer, RPCHook rpcHook) {
		this.defaultMQPullConsumer = defaultMQPullConsumer;
		this.rpcHook = rpcHook;
	}

	public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
		this.consumeMessageHookList.add(hook);
		log.info("register consumeMessageHook, {}", hook.hookName());
	}

	public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
		createTopic(key, newTopic, queueNum, 0);
	}

	public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
		this.isRunning();
		this.mqClientFactory.getMqAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag, null);
	}

	public long fetchConsumeOffset(MessageQueue mq, boolean fromStore) throws MQClientException {
		this.isRunning();
		return this.offsetStore.readOffset(mq, fromStore ? ReadOffsetType.READ_FROM_STORE : ReadOffsetType.MEMORY_FIRST_THEN_STORE);
	}

	public Set<MessageQueue> fetchMessageQueuesInBalance(String topic) throws MQClientException {
		this.isRunning();
		if (topic == null) {
			throw new IllegalArgumentException("topic is null");
		}

		ConcurrentMap<MessageQueue, ProcessQueue> mqTable = this.rebalanceImpl.getProcessQueueTable();
		Set<MessageQueue> mqResult = mqTable.keySet().stream().filter(mq -> mq.getTopic().equals(topic)).collect(Collectors.toSet());

		return parseSubscribeMessageQueues(mqResult);
	}

	public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
		this.isRunning();
		return this.mqClientFactory.getMqAdminImpl().fetchPublishMessageQueues(topic);
	}

	public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
		this.isRunning();
		Set<MessageQueue> result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
		if (result == null) {
			result = this.mqClientFactory.getMqAdminImpl().fetchSubscribeMessageQueues(topic);
		}
		return parseSubscribeMessageQueues(result);
	}

	public Set<MessageQueue> parseSubscribeMessageQueues(Set<MessageQueue> queueSet) {
		return queueSet.stream()
				.map(mq -> {
					String topic = NamespaceUtil.withoutNamespace(mq.getTopic(), this.defaultMQPullConsumer.getNamespace());
					return new MessageQueue(topic, mq.getBrokerName(), mq.getQueueId());
				})
				.collect(Collectors.toSet());
	}

	public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
		this.isRunning();
		return this.mqClientFactory.getMqAdminImpl().earliestMsgStoreTime(mq);
	}

	public long maxOffset(MessageQueue mq) throws MQClientException {
		this.isRunning();
		return this.mqClientFactory.getMqAdminImpl().maxOffset(mq);
	}

	public long minOffset(MessageQueue mq) throws MQClientException {
		this.isRunning();
		return this.mqClientFactory.getMqAdminImpl().minOffset(mq);
	}

	public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums) {
		return pull(mq, subExpression, offset, maxNums, this.defaultMQPullConsumer.getConsumerPullTimeoutMillis());
	}

	public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums, long timeout) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException, MQBrokerException, InterruptedException, RemotingTooMuchRequestException {
		SubscriptionData subscriptionData = getSubscriptionData(mq, subExpression);
		return pullSyncImpl(mq, subscriptionData, offset, maxNums, false, timeout);
	}

	public PullResult pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums) {
		return pull(mq, messageSelector, offset, maxNums, this.defaultMQPullConsumer.getConsumerPullTimeoutMillis());
	}

	public PullResult pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums, long timeout) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException, MQBrokerException, InterruptedException, MQClientException, RemotingTooMuchRequestException {
		SubscriptionData subscriptionData = getSubscriptionData(mq, messageSelector);
		return pullSyncImpl(mq, subscriptionData, offset, maxNums, false, timeout);
	}

	public PullResult pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums) throws MQClientException {
		SubscriptionData subscriptionData = getSubscriptionData(mq, subExpression);
		return pullAsyncImpl(mq, subscriptionData, offset, maxNums, true, this.defaultMQPullConsumer.getConsumePullTimeoutMillis());
	}

	public PullResult pullBlockIfNotFoundWithMessageSelector(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums) throws MQClientException {
		SubscriptionData subscriptionData = getSubscriptionData(mq, messageSelector);
		return pullAsyncImpl(mq, subscriptionData, offset, maxNums, true, this.defaultMQPullConsumer.getConsumePullTimeoutMillis());
	}

	public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback) {
		pull(mq, subExpression, offset, maxNums, pullCallback, this.defaultMQPullConsumer.getConsumerPullTimeoutMillis());
	}

	public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback, long timeout) throws RemotingException, InterruptedException, MQClientException {
		SubscriptionData subscriptionData = getSubscriptionData(mq, subExpression);
		pullAsyncImpl(mq, subscriptionData, offset, maxNums, pullCallback, false, timeout);
	}

	public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, int maxSize, PullCallback pullCallback, long timeout) throws RemotingException, InterruptedException, MQClientException {
		SubscriptionData subscriptionData = getSubscriptionData(mq, subExpression);
		pullAsyncImpl(mq, subscriptionData, offset, maxNums, maxSize, pullCallback, false, timeout);
	}

	public void pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums, PullCallback pullCallback) {
		pull(mq, messageSelector, offset, maxNums, pullCallback, this.defaultMQPullConsumer.getConsumerPullTimeoutMillis());
	}

	public void pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums, PullCallback pullCallback, long timeout) throws MQClientException, RemotingException, InterruptedException {
		SubscriptionData subscriptionData = getSubscriptionData(mq, messageSelector);
		pullAsyncImpl(mq, subscriptionData, offset, maxNums, pullCallback, false, timeout);
	}

	public void pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback) throws MQClientException {
		SubscriptionData subscriptionData = getSubscriptionData(mq, subExpression);
		pullAsyncImpl(mq, subscriptionData, offset, maxNums, pullCallback, true, this.defaultMQPullConsumer.getConsumerPullTimeoutMillis());
	}

	public void pullBlockIfNotFoundWithMessageSelector(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums, PullCallback pullCallback) throws MQClientException {
		SubscriptionData subscriptionData = getSubscriptionData(mq, messageSelector);
		pullAsyncImpl(mq, subscriptionData, offset, maxNums, pullCallback, true, this.defaultMQPullConsumer.getConsumerPullTimeoutMillis());
	}

	public void resetTopic(List<MessageExt> msgList) {
		if (defaultMQPullConsumer.getNamespace() != null) {
			for (MessageExt messageExt : msgList) {
				messageExt.setTopic(NamespaceUtil.withoutNamespace(messageExt.getTopic(), this.defaultMQPullConsumer.getNamespace()));
			}
		}
	}

	public void subscriptionAutomatically(final String topic) {
		if (!this.rebalanceImpl.getSubscriptionInner().containsKey(topic)) {
			try {
				SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, SubscriptionData.SUB_ALL);
				this.rebalanceImpl.subscriptionInner.putIfAbsent(topic, subscriptionData);
			}
			catch (Exception ignored) {}
		}
	}

	public void unsubscribe(String topic) {
		this.rebalanceImpl.getSubscriptionInner().remove(topic);
	}

	public void executeHookBefore(final ConsumeMessageContext context) {
		if (!this.consumeMessageHookList.isEmpty()) {
			for (ConsumeMessageHook hook : this.consumeMessageHookList) {
				try {
					hook.consumeMessageBefore(context);
				}
				catch (Exception ignored) {}
			}
		}
	}

	public void executeHookAfter(final ConsumeMessageContext context) {
		if (!this.consumeMessageHookList.isEmpty()) {
			for (ConsumeMessageHook hook : this.consumeMessageHookList) {
				try {
					hook.consumeMessageAfter(context);
				}
				catch (Exception ignored) {}
			}
		}
	}

	public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end) throws InterruptedException, MQClientException {
		this.isRunning();
		return this.mqClientFactory.getMqAdminImpl().queryMessage(topic, key, maxNum, begin, end);
	}

	public MessageExt queryMessageByUniqKey(String topic, String uniqKey) throws InterruptedException, MQClientException {
		this.isRunning();
		return this.mqClientFactory.getMqAdminImpl().queryMessageByUniqKey(topic, uniqKey);
	}

	public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
		this.isRunning();
		return this.mqClientFactory.getMqAdminImpl().searchOffset(mq, timestamp);
	}

	public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName) {
		sendMessageBack(msg, delayLevel, brokerName, this.defaultMQPullConsumer.getConsumeGroup());
	}

	public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName, String consumerGroup) {
		try {
			String destBrokerName = brokerName;
			if (destBrokerName)
		}
		catch (Exception e) {
			log.error("sendMessageBack exception, {}", this.defaultMQPullConsumer.getConsumeGroup(), e);

			Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPullConsumer.getConsumeGroup()), msg.getBody());
			String originMessageId = MessageAccessor.getOriginMessageId(msg);
			MessageAccessor.setOriginMessageId(msg, UtilAll.isBlank(originMessageId) ? msg.getMsgId() : originMessageId);
			newMsg.setFlag(msg.getFlag());
			MessageAccessor.setProperties(newMsg, msg.getProperties());
			MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
			MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes() + 1));
			MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(this.defaultMQPullConsumer.getMaxReconsumeTimes()));
			newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());
			this.mqClientFactory.getDefaultMQProducer().send(newMsg);
		}
		finally {
			msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPullConsumer.getNamespace()));
		}
	}

	@Override
	public String groupName() {
		return this.defaultMQPullConsumer.getConsumeGroup();
	}

	@Override
	public MessageModel messageModel() {
		return this.defaultMQPullConsumer.getMessageModel();
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
		Set<SubscriptionData> result = new HashSet<>();

		Set<String> topics = this.defaultMQPullConsumer.getRegisterTopics();
		if (topics != null) {
			synchronized (topics) {
				for (String t : topics) {
					SubscriptionData subscriptionData = null;
					try {
						subscriptionData = FilterAPI.buildSubscriptionData(t, SubscriptionData.SUB_ALL);
					}
					catch (Exception e) {
						log.error("parse subscription error", e);
					}
					if (subscriptionData != null) {
						subscriptionData.setSubVersion(0L);
						result.add(subscriptionData);
					}
				}
			}
		}

		return result;
	}

	@Override
	public void doBalance() {
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
			this.isRunning();
			Set<MessageQueue> mqs = new HashSet<>();
			Set<MessageQueue> allocateMQ = this.rebalanceImpl.getProcessQueueTable().keySet();
			mqs.addAll(allocateMQ);
			this.offsetStore.persistAll(mqs);
		}
		catch (Exception e) {
			log.error("group: {} persistConsumerOffset exception", this.defaultMQPullConsumer.getConsumeGroup(), e);
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
		return this.defaultMQPullConsumer.isUnitMode();
	}

	@Override
	public ConsumerRunningInfo consumerRunningInfo() {
		ConsumerRunningInfo info = new ConsumerRunningInfo();

		Properties properties = MixAll.object2Properties(this.defaultMQPullConsumer);
		properties.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, String.valueOf(this.consumerStartTimestamp));

		info.getSubscriptionSet().addAll(this.subscriptions());

		return info;
	}

	private void isRunning() throws MQClientException {
		if (this.serviceState != ServiceState.RUNNING) {
			throw new MQClientException("The consumer is not in running status, " + this.serviceState + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK), null);
		}
	}

	private SubscriptionData getSubscriptionData(MessageQueue mq, String subExpression) throws MQClientException {
		if (mq == null) {
			throw new MQClientException("mq is null", null);
		}

		try {
			return FilterAPI.buildSubscriptionData(mq.getTopic(), subExpression);
		}
		catch (Exception e) {
			throw new MQClientException("parse subscription error", e);
		}
	}

	private SubscriptionData getSubscriptionData(MessageQueue mq, MessageSelector messageSelector) throws MQClientException {
		if (mq == null) {
			throw new MQClientException("mq is null", null);
		}

		try {
			return FilterAPI.build(mq.getTopic(), messageSelector.getExpression(), messageSelector.getType());
		}
		catch (Exception e) {
			throw new MQClientException("parse subscription error", e);
		}
	}

	private PullResult pullSyncImpl(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums, boolean block, long timeout) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException, MQBrokerException, InterruptedException, RemotingTooMuchRequestException {
		this.isRunning();
		if (mq == null) {
			throw new MQClientException("mq is null", null);
		}
		if (offset < 0) {
			throw new MQClientException("offset < 0", null);
		}
		if (maxNums <= 0) {
			throw new MQClientException("maxNums <= 0", null);
		}

		this.subscriptionAutomatically(mq.getTopic());

		int sysFlag = PullSysFlag.buildSysFlag(false, block, true, false);
		long timeoutMillis = block ? this.defaultMQPullConsumer.getConsumerTimeoutMillisWhenSuspend() : timeout;
		boolean isTagType = ExpressionType.isTagType(subscriptionData.getExpressionType());

		PullResult pullResult = this.pullAPIWrapper.pullKernelImpl(mq, subscriptionData.getSubString(), subscriptionData.getExpressionType(), isTagType ? 0L : subscriptionData.getSubVersion(),
				offset, maxNums, sysFlag, 0, this.defaultMQPullConsumer.getBrokerSuspendMaxTimeMillis(), timeoutMillis, CommunicationMode.SYNC, null);

		this.pullAPIWrapper.processPullResult(mq, pullResult, subscriptionData);

		this.resetTopic(pullResult.getMsgFoundList());

		if (!this.consumeMessageHookList.isEmpty()) {
			ConsumeMessageContext context = new ConsumeMessageContext();
			context.setNamespace(defaultMQPullConsumer.getNamespace());
			context.setConsumerGroup(this.groupName());
			context.setMq(mq);
			context.setMsgList(pullResult.getMsgFoundList());
			context.setSuccess(false);

			this.executeHookBefore(context);

			context.setStatus(ConsumeConcurrentlyStatus.CONSUME_SUCCESS.toString());
			context.setSuccess(true);
			context.setAccessChannel(defaultMQPullConsumer.getAccessChannel());

			this.executeHookAfter(context);
		}
		return pullResult;
	}

	private void pullAsyncImpl(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums, PullCallback pullCallback, boolean block, long timeout) throws RemotingException, InterruptedException, MQClientException {
		pullAsyncImpl(mq, subscriptionData, offset, maxNums, Integer.MAX_VALUE, pullCallback, block, timeout);
	}

	private void pullAsyncImpl(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums, int maxSizeInBytes, PullCallback pullCallback, boolean block, long timeout) throws MQClientException, RemotingException, InterruptedException {
		this.isRunning();
		if (mq == null) {
			throw new MQClientException("mq is null", null);
		}
		if (offset < 0) {
			throw new MQClientException("offset < 0", null);
		}
		if (maxNums <= 0) {
			throw new MQClientException("maxNums <= 0", null);
		}
		if (maxSizeInBytes <= 0) {
			throw new MQClientException("maxSizeInBytes <= 0", null);
		}
		if (pullCallback == null) {
			throw new MQClientException("pullCallback is null", null);
		}

		this.subscriptionAutomatically(mq.getTopic());

		try {
			int sysFlag = PullSysFlag.buildSysFlag(false, block, true, false);
			long timeoutMillis = block ? this.defaultMQPullConsumer.getConsumerTimeoutMillisWhenSuspend() : timeout;
			boolean isTagType = ExpressionType.isTagType(subscriptionData.getExpressionType());

			this.pullAPIWrapper.pullKernelImpl(mq, subscriptionData.getSubString(), subscriptionData.getExpressionType(), isTagType ? 0L : subscriptionData.getSubVersion(),
					offset, maxNums, maxSizeInBytes, sysFlag, 0, this.defaultMQPullConsumer.getBrokerSuspendMaxTimeMillis(),
					timeoutMillis, CommunicationMode.ASYNC, new PullCallback() {
						@Override
						public void onSuccess(PullResult pullResult) {
							PullResult userPullResult = DefaultMQPullConsumerImpl.this.pullAPIWrapper.processPullResult(mq, pullResult, subscriptionData);
							resetTopic(userPullResult.getMsgFoundList());
							pullCallback.onSuccess(userPullResult);
						}

						@Override
						public void onException(Throwable e) {
							pullCallback.onException(e);
						}
					});
		}
		catch (MQBrokerException e) {
			throw new MQClientException("pullAsync unknown exception", e);
		}
	}
}
