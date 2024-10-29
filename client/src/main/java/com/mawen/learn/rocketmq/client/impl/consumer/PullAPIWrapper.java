package com.mawen.learn.rocketmq.client.impl.consumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.mawen.learn.rocketmq.client.consumer.PopCallback;
import com.mawen.learn.rocketmq.client.consumer.PullCallback;
import com.mawen.learn.rocketmq.client.consumer.PullResult;
import com.mawen.learn.rocketmq.client.consumer.PullStatus;
import com.mawen.learn.rocketmq.client.exception.MQBrokerException;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.hook.FilterMessageContext;
import com.mawen.learn.rocketmq.client.hook.FilterMessageHook;
import com.mawen.learn.rocketmq.client.impl.CommunicationMode;
import com.mawen.learn.rocketmq.client.impl.FindBrokerResult;
import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.common.MQVersion;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.filter.ExpressionType;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageAccessor;
import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.message.MessageDecoder;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.common.sysflag.MessageSysFlag;
import com.mawen.learn.rocketmq.common.sysflag.PullSysFlag;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingConnectException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingSendRequestException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTimeoutException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.mawen.learn.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import com.mawen.learn.rocketmq.remoting.protocol.route.TopicRouteData;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/29
 */
@Getter
@Setter
public class PullAPIWrapper {
	private static final Logger log = LoggerFactory.getLogger(PullAPIWrapper.class);

	private final MQClientInstance mqClientFactory;
	private final String consumerGroup;
	private final boolean unitMode;
	private ConcurrentMap<MessageQueue, AtomicLong> pullFromWhichNodeTable = new ConcurrentHashMap<>();
	private volatile boolean connectBrokerByUser = false;
	private volatile long defaultBrokerId = MixAll.MASTER_ID;
	private Random random = new Random(System.nanoTime());
	private List<FilterMessageHook> filterMessageHookList = new ArrayList<>();

	public PullAPIWrapper(MQClientInstance mqClientFactory, String consumerGroup, boolean unitMode) {
		this.mqClientFactory = mqClientFactory;
		this.consumerGroup = consumerGroup;
		this.unitMode = unitMode;
	}

	public PullResult processPullResult(final MessageQueue mq, final PullResult pullResult, final SubscriptionData subscriptionData) {
		PullResultExt pullResultExt = (PullResultExt) pullResult;

		this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());

		if (pullResult.getPullStatus() == PullStatus.FOUND) {
			ByteBuffer buffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());
			List<MessageExt> msgList = MessageDecoder.decodeBatch(buffer, this.mqClientFactory.getClientConfig().isDecodeReadBody(), this.mqClientFactory.getClientConfig().isDecodeDecompressBody(), true);

			boolean needDecodeInnerMessage = false;
			for (MessageExt msg : msgList) {
				if (MessageSysFlag.check(msg.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG) && MessageSysFlag.check(msg.getSysFlag(), MessageSysFlag.NEED_UNWRAP_FLAG)) {
					needDecodeInnerMessage = true;
					break;
				}
			}

			if (needDecodeInnerMessage) {
				List<MessageExt> innerMsgList = new ArrayList<>();
				try {
					for (MessageExt msg : msgList) {
						if (MessageSysFlag.check(msg.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG) && MessageSysFlag.check(msg.getSysFlag(), MessageSysFlag.NEED_UNWRAP_FLAG)) {
							MessageDecoder.decodeMessages(msg, innerMsgList);
						}
						else {
							innerMsgList.add(msg);
						}
					}
					msgList = innerMsgList;
				}
				catch (Throwable t) {
					log.error("Try to decode the inner batch failed for {}", pullResult, t);
				}
			}

			List<MessageExt> msgListFilterAgain = msgList;
			if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()) {
				msgListFilterAgain = new ArrayList<>(msgList.size());
				for (MessageExt msg : msgList) {
					if (msg.getTags() != null) {
						if (subscriptionData.getTagsSet().contains(msg.getTags())) {
							msgListFilterAgain.add(msg);
						}
					}
				}
			}

			if (this.hasHook()) {
				FilterMessageContext context = new FilterMessageContext();
				context.setUnitMode(unitMode);
				context.setMsgList(msgListFilterAgain);
				this.executeHook(context);
			}

			for (MessageExt msg : msgListFilterAgain) {
				String traFlag = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
				if (Boolean.parseBoolean(traFlag)) {
					msg.setTransactionId(msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
				}
				MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MIN_OFFSET, Long.toString(pullResult.getMinOffset()));
				MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_OFFSET, Long.toString(pullResult.getMaxOffset()));
				msg.setBrokerName(mq.getBrokerName());
				msg.setQueueId(mq.getQueueId());
				if (pullResultExt.getOffsetDelta() != null) {
					msg.setQueueOffset(pullResultExt.getOffsetDelta() + msg.getQueueOffset());
				}
			}

			pullResultExt.setMsgFoundList(msgListFilterAgain);
		}
		pullResultExt.setMessageBinary(null);

		return pullResult;
	}

	public void updatePullFromWhichNode(final MessageQueue mq, final long brokerId) {
		this.pullFromWhichNodeTable.computeIfAbsent(mq, k -> new AtomicLong()).set(brokerId);
	}

	public boolean hasHook() {
		return !this.filterMessageHookList.isEmpty();
	}

	public void executeHook(final FilterMessageContext context) {
		if (!this.filterMessageHookList.isEmpty()) {
			for (FilterMessageHook hook : this.filterMessageHookList) {
				try {
					hook.filterMessage(context);
				}
				catch (Throwable t) {
					log.error("execute hook error, hookName={}", hook.hookName());
				}
			}
		}
	}

	public PullResult pullKernelImpl(final MessageQueue mq, final String subExpression, final String expressionType, final long subVersion, final long offset, final int maxNums,
			final int maxSizeInBytes, final int sysFlag, final long commitOffset, final long brokerSuspendMaxTimeMillis, final long timeoutMillis, final CommunicationMode communicationMode, final PullCallback pullCallback) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException, MQBrokerException, InterruptedException, RemotingTooMuchRequestException {
		FindBrokerResult findBrokerResult = this.mqClientFactory.findBrokerAddressInSubscribe(this.mqClientFactory.getBrokerNameFromMessageQueue(mq), this.recalculatePullFromWhichNode(mq), false);
		if (findBrokerResult == null) {
			this.mqClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
			findBrokerResult = this.mqClientFactory.findBrokerAddressInSubscribe(this.mqClientFactory.getBrokerNameFromMessageQueue(mq), this.recalculatePullFromWhichNode(mq), false);
		}

		if (findBrokerResult == null) {
			if (!ExpressionType.isTagType(expressionType) && findBrokerResult.getBrokerVersion() < MQVersion.Version.V4_1_0_SNAPSHOT.ordinal()) {
				throw new MQClientException("The broker[" + mq.getBrokerName() + ", " + findBrokerResult.getBrokerVersion() + "] does not upgrade to support filter message by " + expressionType, null);
			}
			int sysFlagInner = sysFlag;

			if (findBrokerResult.isSlave()) {
				sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
			}

			PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
			requestHeader.setConsumerGroup(this.consumerGroup);
			requestHeader.setTopic(mq.getTopic());
			requestHeader.setQueueId(mq.getQueueId());
			requestHeader.setQueueOffset(offset);
			requestHeader.setMaxMsgNums(maxNums);
			requestHeader.setSysFlag(sysFlagInner);
			requestHeader.setCommitOffset(commitOffset);
			requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);
			requestHeader.setSubscription(subExpression);
			requestHeader.setSubVersion(subVersion);
			requestHeader.setMaxMsgBytes(maxSizeInBytes);
			requestHeader.setExpressionType(expressionType);
			requestHeader.setBrokerName(mq.getBrokerName());

			String brokerAddr = findBrokerResult.getBrokerAddr();
			if (PullSysFlag.hasClassFilterFlag(sysFlagInner)) {
				brokerAddr = computePullFromWhichFilterServer(mq.getTopic(), brokerAddr);
			}

			PullResult pullResult = this.mqClientFactory.getMqClientAPIImpl().pullMessage(brokerAddr, requestHeader, timeoutMillis, communicationMode, pullCallback);

			return pullResult;
		}

		throw new MQClientException("The broker[" + mq.getBrokerName() + "] no exist", null);
	}

	public PullResult pullKernelImpl(MessageQueue mq, final String subExpression, final String expressionType, final long subVersion, long offset, final int maxNums, final int sysFlag,
			long commitOffset, final long brokerSuspendMaxTimeMillis, final long timeoutMillis, final CommunicationMode communicationMode, PullCallback pullCallback) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException, MQBrokerException, InterruptedException, MQClientException, RemotingTooMuchRequestException {
		return pullKernelImpl(mq, subExpression, expressionType, subVersion, offset, maxNums, Integer.MAX_VALUE, sysFlag, commitOffset, brokerSuspendMaxTimeMillis, timeoutMillis, communicationMode, pullCallback);
	}

	public long recalculatePullFromWhichNode(final MessageQueue mq) {
		if (this.isConnectBrokerByUser()) {
			return this.defaultBrokerId;
		}

		AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
		if (suggest != null) {
			return suggest.get();
		}
		return MixAll.MASTER_ID;
	}

	private String computePullFromWhichFilterServer(final String topic, final String brokerAddr) throws MQClientException {
		ConcurrentMap<String, TopicRouteData> topicRouteTable = this.mqClientFactory.getTopicRouteTable();
		if (topicRouteTable != null) {
			TopicRouteData topicRouteData = topicRouteTable.get(topic);
			List<String> list = topicRouteData.getFilterServerTable().get(brokerAddr);

			if (CollectionUtils.isNotEmpty(list)) {
				return list.get(randomNum() % list.size());
			}
		}

		throw new MQClientException("Find Filter Server Failed, Broker Addr: " + brokerAddr + " topic: " + topic, null);
	}

	public int randomNum() {
		int value = random.nextInt();
		if (value < 0) {
			value = Math.abs(value);
			if (value < 0) {
				value = 0;
			}
		}
		return value;
	}

	public void registerFilterMessageHook(List<FilterMessageHook> hooks) {
		this.filterMessageHookList = hooks;
	}

	public void popAsync(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup, long timeout,
			PopCallback popCallback, boolean poll, int initMode, boolean order, String expressionType, String expression) throws MQClientException {
		FindBrokerResult findBrokerResult = this.mqClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
		if (findBrokerResult == null) {
			this.mqClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
			findBrokerResult = this.mqClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
		}

		if (findBrokerResult != null) {
			PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
			requestHeader.setConsumerGroup(consumerGroup);
			requestHeader.setTopic(mq.getTopic());
			requestHeader.setQueueId(mq.getQueueId());
			requestHeader.setMaxMsgNums(maxNums);
			requestHeader.setInvisibleTime(invisibleTime);
			requestHeader.setInitMode(initMode);
			requestHeader.setExpType(expressionType);
			requestHeader.setExp(expression);
			requestHeader.setOrder(order);
			requestHeader.setBrokerName(mq.getBrokerName());
			if (poll) {
				requestHeader.setPollTime(timeout);
				requestHeader.setBronTime(System.currentTimeMillis());
				timeout += 10 * 1000;
			}
			String brokerAddr = findBrokerResult.getBrokerAddr();
			this.mqClientFactory.getMqClientAPIImpl().popMessageAsync(mq.getBrokerName(), brokerAddr, requestHeader, timeout, popCallback);
			return;
		}
		throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
	}
}
