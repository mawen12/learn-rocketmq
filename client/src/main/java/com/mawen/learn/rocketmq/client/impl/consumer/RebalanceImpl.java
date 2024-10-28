package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mawen.learn.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.impl.FindBrokerResult;
import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.common.KeyBuilder;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.common.message.MessageQueueAssignment;
import com.mawen.learn.rocketmq.common.message.MessageRequestMode;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTimeoutException;
import com.mawen.learn.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import com.mawen.learn.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import com.mawen.learn.rocketmq.remoting.protocol.filter.FilterAPI;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.MessageModel;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/28
 */
@Getter
@Setter
public abstract class RebalanceImpl {
	protected static final Logger log = LoggerFactory.getLogger(RebalanceImpl.class);

	private static final int TIMEOUT_CHECK_TIMES = 3;
	private static final int QUERY_ASSIGNMENT_TIMEOUT = 3000;

	protected final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = new ConcurrentHashMap<>();
	protected final ConcurrentMap<MessageQueue, PopProcessQueue> popProcessQueueTable = new ConcurrentHashMap<>();
	protected final ConcurrentMap<String, Set<MessageQueue>> topicSubscribeInfoTable = new ConcurrentHashMap<>();
	protected final ConcurrentMap<String, SubscriptionData> subscriptionInner = new ConcurrentHashMap<>();

	protected String consumerGroup;
	protected MessageModel messageModel;
	protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;
	protected MQClientInstance mqClientFactory;

	private ConcurrentMap<String, String> topicBrokerRebalance = new ConcurrentHashMap<>();
	private ConcurrentMap<String, String> topicClientRebalance = new ConcurrentHashMap<>();

	public RebalanceImpl(String consumerGroup, MessageModel messageModel, AllocateMessageQueueStrategy allocateMessageQueueStrategy, MQClientInstance mqClientFactory) {
		this.consumerGroup = consumerGroup;
		this.messageModel = messageModel;
		this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
		this.mqClientFactory = mqClientFactory;
	}

	public void removeProcessQueue(final MessageQueue mq) {
		ProcessQueue prev = this.processQueueTable.remove(mq);
		if (prev != null) {
			boolean dropped = prev.isDropped();
			prev.setDropped(true);
			this.removeUnnecessaryMessageQueue(mq, prev);
			log.info("Fix Offset, {}, remove unnecessary mq, {} Dropped: {}", consumerGroup, mq, dropped);
		}
	}

	public boolean lock(final MessageQueue mq) {
		FindBrokerResult findBrokerResult = this.mqClientFactory.findBrokerAddressInSubscribe(this.mqClientFactory.getBrokerNameFromMessageQueue(mq), MixAll.MASTER_ID, true);
		if (findBrokerResult != null) {
			LockBatchRequestBody requestBody = new LockBatchRequestBody();
			requestBody.setConsumerGroup(this.consumerGroup);
			requestBody.setClientId(this.mqClientFactory.getClientId());
			requestBody.getMqSet().add(mq);

			try {
				Set<MessageQueue> lockedMQ = this.mqClientFactory.getMqClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
				for (MessageQueue mmqq : lockedMQ) {
					ProcessQueue processQueue = this.processQueueTable.get(mmqq);
					if (processQueue != null) {
						processQueue.setLocked(true);
						processQueue.setLastLockTimestamp(System.currentTimeMillis());
					}
				}

				boolean lockOK = lockedMQ.contains(mq);
				log.info("message queue lock {}, {} {}", lockOK ? "OK" : "Failed", consumerGroup, mq);
				return lockOK;
			}
			catch (Exception e) {
				log.error("lockBatchMQ exception. {}", mq, e);
			}
		}

		return false;
	}

	public void unlock(final MessageQueue mq, final boolean oneway) {
		FindBrokerResult findBrokerResult = this.mqClientFactory.findBrokerAddressInSubscribe(this.mqClientFactory.getBrokerNameFromMessageQueue(mq), MixAll.MASTER_ID, true);
		if (findBrokerResult != null) {
			UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
			requestBody.setConsumerGroup(consumerGroup);
			requestBody.setClientId(this.mqClientFactory.getClientId());
			requestBody.getMqSet().add(mq);

			try {
				this.mqClientFactory.getMqClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);
				log.info("unlock messageQueue, group: {}, clientId: {}, mq: {}", consumerGroup, this.mqClientFactory.getClientId(), mq);
			}
			catch (Exception e) {
				log.error("unlockBatchMQ exception. {}", mq,e);
			}
		}
	}

	public void lockAll() {
		Map<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();
		Iterator<Map.Entry<String, Set<MessageQueue>>> it = brokerMqs.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, Set<MessageQueue>> entry = it.next();
			String brokerName = entry.getKey();
			Set<MessageQueue> mqs = entry.getValue();

			if (mqs.isEmpty()) {
				continue;
			}

			FindBrokerResult findBrokerResult = this.mqClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
			if (findBrokerResult != null) {
				LockBatchRequestBody requestBody = new LockBatchRequestBody();
				requestBody.setConsumerGroup(consumerGroup);
				requestBody.setClientId(this.mqClientFactory.getClientId());
				requestBody.setMqSet(mqs);

				try {
					Set<MessageQueue> lockOKMQSet = this.mqClientFactory.getMqClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
					for (MessageQueue mq : mqs) {
						ProcessQueue pq = this.processQueueTable.get(mq);
						if (pq != null) {
							if (lockOKMQSet.contains(mq)) {
								if (!pq.isLocked()) {
									log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
								}
								pq.setLocked(true);
								pq.setLastLockTimestamp(System.currentTimeMillis());
							}
							else {
								pq.setLocked(false);
								log.warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
							}
						}
					}
				}
				catch (Exception e) {
					log.error("lockBatchMQ exception, {}", mqs, e);
				}
			}
		}
	}

	public void unlockAll(final boolean oneway) {
		Map<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();
		for (Map.Entry<String, Set<MessageQueue>> entry : brokerMqs.entrySet()) {
			String brokerName = entry.getKey();
			Set<MessageQueue> mqs = entry.getValue();

			if (mqs.isEmpty()) {
				continue;
			}

			FindBrokerResult findBrokerResult = this.mqClientFactory.findBrokerAddresInSubscribe(brokerName, MixAll.MASTER_ID, true);
			if (findBrokerResult != null) {
				UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
				requestBody.setConsumerGroup(consumerGroup);
				requestBody.setClientId(this.mqClientFactory.getClientId());
				requestBody.setMqSet(mqs);

				try {
					this.mqClientFactory.getMqClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);

					for (MessageQueue mq : mqs) {
						ProcessQueue pq = this.processQueueTable.get(mq);
						if (pq != null) {
							pq.setLocked(false);
							log.info("the message queue unlock OK, Group: {} {}", consumerGroup, mq);
						}
					}
				}
				catch (Exception e) {
					log.error("unlockBatchMQ exception, {}", mqs, e);
				}
			}
		}
	}

	public boolean clientRebalance(String topic) {
		return true;
	}

	public boolean doRebalance(final boolean isOrder) {
		boolean balanced = true;
		ConcurrentMap<String, SubscriptionData> subTable = this.getSubscriptionInner();
		if (subTable != null) {
			for (Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
				String topic = entry.getKey();
				try {
					if (!clientRebalance(topic) && tryQueryAssignment(topic)) {
						boolean result = this.getRebalanceResultFromBroker(topic, isOrder);
						if (!result) {
							balanced = false;
						}
					}
					else {
						boolean result = this.rebalanceByTopic(topic, isOrder);
						if (!result) {
							balanced = false;
						}
					}
				}
				catch (Throwable e) {
					if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
						log.warn("rebalance exception", e);
						balanced = false;
					}
				}
			}
		}

		this.truncateMessageQueueNotMyTopic();

		return balanced;
	}

	public void destroy() {
		Iterator<Map.Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<MessageQueue, ProcessQueue> next = it.next();
			next.getValue().setDropped(true);
		}

		this.processQueueTable.clear();

		Iterator<Map.Entry<MessageQueue, PopProcessQueue>> popIt = this.popProcessQueueTable.entrySet().iterator();
		while (popIt.hasNext()) {
			Map.Entry<MessageQueue, PopProcessQueue> next = popIt.next();
			next.getValue().setDropped(true);
		}

		this.popProcessQueueTable.clear();
	}

	public boolean removeUnnecessaryMessageQueue(final MessageQueue mq, final PopProcessQueue pp) {
		return true;
	}

	public abstract void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll, final Set<MessageQueue> mqDivided);

	public abstract boolean removeUnnecessaryMessageQueue(final MessageQueue mq, final ProcessQueue pq);

	public abstract ConsumeType consumeType();

	public abstract void removeDirtyOffset(final MessageQueue mq);

	public abstract long computePullFromWhere(final MessageQueue mq);

	public abstract long computePullFromWhereWithException(final MessageQueue mq) throws MQClientException;

	public abstract int getConsumeInitMode();

	public abstract void dispatchPullRequest(final List<PullRequest> pullRequestList, final long delay);

	public abstract void dispatchPopPullRequest(final List<PopRequest> pullRequestList, final long delay);

	public abstract ProcessQueue createProcessQueue();

	public abstract PopProcessQueue createPopProcessQueue();

	private Map<String, Set<MessageQueue>> buildProcessQueueTableByBrokerName() {
		Map<String, Set<MessageQueue>> result = new HashMap<>();
		for (Map.Entry<MessageQueue, ProcessQueue> entry : this.processQueueTable.entrySet()) {
			MessageQueue mq = entry.getKey();
			ProcessQueue pq = entry.getValue();

			if (pq.isDropped()) {
				continue;
			}

			String brokerName = this.mqClientFactory.getBrokerNameFromMessageQueue(mq);
			Set<MessageQueue> mqs = result.get(brokerName);
			if (mqs == null) {
				mqs = new HashSet<>();
				result.put(mq.getBrokerName(), mqs);
			}

			mqs.add(mq);
		}

		return result;
	}

	private boolean tryQueryAssignment(String topic) {
		if (topicClientRebalance.containsKey(topic)) {
			return false;
		}

		if (topicBrokerRebalance.containsKey(topic)) {
			return true;
		}

		String strategyName = allocateMessageQueueStrategy != null ? allocateMessageQueueStrategy.getName() : null;
		int retryTimes = 0;
		while (retryTimes++ < TIMEOUT_CHECK_TIMES) {
			try {
				Set<MessageQueueAssignment> resultSet = mqClientFactory.queryAssignment(topic, consumerGroup, strategyName, messageModel, QUERY_ASSIGNMENT_TIMEOUT / TIMEOUT_CHECK_TIMES * retryTimes);
				topicBrokerRebalance.put(topic, topic);
				return true;
			}
			catch (Throwable t) {
				if (!(t instanceof RemotingTimeoutException)) {
					log.error("tryQueryAssignment error, ", t);
					topicClientRebalance.put(topic, topic);
					return false;
				}
			}
		}

		if (retryTimes >= TIMEOUT_CHECK_TIMES) {
			topicClientRebalance.put(topic, topic);
			return false;
		}
		return true;
	}

	private boolean rebalanceByTopic(final String topic, final boolean isOrder) {
		boolean balanced = true;
		switch (messageModel) {
			case BROADCASTING: {
				Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
				if (mqSet != null) {
					boolean changed = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
					if (changed) {
						this.messageQueueChanged(topic, mqSet, mqSet);
						log.info("messageQueueChanged {} {} {} {}", consumerGroup, topic, mqSet, mqSet);
					}

					balanced = mqSet.equals(getWorkingMessageQueue(topic));
				}
				else {
					this.messageQueueChanged(topic, Collections.emptySet(), Collections.emptySet());
					log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
				}
				break;
			}
			case CLUSTERING: {
				Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
				List<String> cidAll = this.mqClientFactory.findConsumerIdList(topic, consumerGroup);
				if (mqSet == null) {
					if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
						this.messageQueueChanged(topic, Collections.emptySet(), Collections.emptySet());
						log.warn("doRebalance, {}, but the topic[{}] not exist", consumerGroup, topic);
					}
				}

				if (cidAll == null) {
					log.warn("doRebalance, {}, {}, get consumer id list failed", consumerGroup, topic);
				}

				if (mqSet != null && cidAll != null) {
					List<MessageQueue> mqAll = new ArrayList<>();
					mqAll.addAll(mqSet);

					Collections.sort(mqAll);
					Collections.sort(cidAll);

					AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;

					List<MessageQueue> allocateResult = null;
					try {
						allocateResult = strategy.allocate(this.consumerGroup, this.mqClientFactory.getClientId(), mqAll, cidAll);
					}
					catch (Exception e) {
						log.error("allocate message queue exception. strategy name: {}, ex: {}", strategy.getName(), e0);
						return false;
					}

					Set<MessageQueue> allocateResultSet = new HashSet<>();
					if (allocateResult != null) {
						allocateResultSet.addAll(allocateResult);
					}

					boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);
					if (changed) {
						log.info("client rebalanced result changed, allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, rebalanceResultSet={}",
								strategy.getName(), consumerGroup, topic, this.mqClientFactory.getClientId(), mqSet.size(), cidAll.size(), allocateResultSet.size(), allocateResultSet);
						this.messageQueueChanged(topic, mqSet, allocateResultSet);
					}

					balanced = allocateResultSet.equals(getWorkingMessageQueue(topic));
				}
				break;
			}
			default:
				break;
		}

		return balanced;
	}

	private boolean getRebalanceResultFromBroker(String topic, boolean isOrder) {
		String strategyName = this.allocateMessageQueueStrategy.getName();
		Set<MessageQueueAssignment> messageQueueAssignments;
		try {
			messageQueueAssignments = this.mqClientFactory.queryAssignment(topic, consumerGroup, strategyName, messageModel, QUERY_ASSIGNMENT_TIMEOUT);
		}
		catch (Exception e) {
			log.error("allocate message queue exception. strategy name: {}, ex: {}", strategyName, e);
			return false;
		}

		if (messageQueueAssignments == null) {
			return false;
		}

		Set<MessageQueue> mqSet = new HashSet<>();
		for (MessageQueueAssignment messageQueueAssignment : messageQueueAssignments) {
			if (messageQueueAssignment.getMessageQueue() != null) {
				mqSet.add(messageQueueAssignment.getMessageQueue());
			}
		}

		Set<MessageQueue> mqAll = null;
		boolean changed = this.updateMessageQueueAssignment(topic, messageQueueAssignments, isOrder);
		if (changed) {
			log.info("broker rebalanced result changed, allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, assignmentSet={}",
					strategyName, consumerGroup, topic, this.mqClientFactory.getClientId(), messageQueueAssignments);
			this.messageQueueChanged(topic, mqAll, mqSet);
		}

		return mqSet.equals(getWorkingMessageQueue(topic));
	}

	private Set<MessageQueue> getWorkingMessageQueue(String topic) {
		Set<MessageQueue> queueSet = new HashSet<>();
		for (Map.Entry<MessageQueue, ProcessQueue> entry : this.processQueueTable.entrySet()) {
			MessageQueue mq = entry.getKey();
			ProcessQueue pq = entry.getValue();

			if (mq.getTopic().equals(topic) && !pq.isDropped()) {
				queueSet.add(mq);
			}
		}

		for (Map.Entry<MessageQueue, PopProcessQueue> entry : this.popProcessQueueTable.entrySet()) {
			MessageQueue mq = entry.getKey();
			PopProcessQueue pq = entry.getValue();

			if (mq.getTopic().equals(topic) && !pq.isDropped()) {
				queueSet.add(mq);
			}
		}

		return queueSet;
	}

	private void truncateMessageQueueNotMyTopic() {
		ConcurrentMap<String, SubscriptionData> subTable = this.getSubscriptionInner();

		for (MessageQueue mq : this.processQueueTable.keySet()) {
			if (!(subTable.containsKey(mq.getTopic()))) {
				ProcessQueue pq = this.processQueueTable.remove(mq);
				if (pq != null) {
					pq.setDropped(true);
					log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, mq);
				}
			}
		}

		for (MessageQueue mq : this.popProcessQueueTable.keySet()) {
			if (!subTable.containsKey(mq.getTopic())) {
				PopProcessQueue pq = this.popProcessQueueTable.remove(mq);
				if (pq != null) {
					pq.setDropped(true);
					log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary pop mq, {}", consumerGroup, mq);
				}
			}
		}

		Iterator<Map.Entry<String, String>> clientIter = topicClientRebalance.entrySet().iterator();
		while (clientIter.hasNext()) {
			if (!subTable.containsKey(clientIter.next().getKey())) {
				clientIter.remove();
			}
		}

		Iterator<Map.Entry<String, String>> brokerIter = topicBrokerRebalance.entrySet().iterator();
		while (brokerIter.hasNext()) {
			if (!subTable.containsKey(brokerIter.next().getKey())) {
				brokerIter.remove();
			}
		}
	}

	private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet, final boolean isOrder) {
		boolean changed = false;
		Map<MessageQueue, ProcessQueue> removeQueueMap = new HashMap<>(this.processQueueTable.size());
		Iterator<Map.Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry<MessageQueue, ProcessQueue> next = it.next();
			MessageQueue mq = next.getKey();
			ProcessQueue pq = next.getValue();

			if (mq.getTopic().equals(topic)) {
				if (!mqSet.contains(mq)) {
					pq.setDropped(true);
					removeQueueMap.put(mq, pq);
				}
				else if (pq.isPullExpired() && this.consumeType() == ConsumeType.CONSUME_PASSIVELY) {
					pq.setDropped(true);
					removeQueueMap.put(mq, pq);
					log.error("[BUG]doRebalance, {}, try remove unnecessary mq, {}, because pull is pause, so try to fixed it", consumerGroup, mq);
				}
			}
		}

		for (Map.Entry<MessageQueue, ProcessQueue> entry : removeQueueMap.entrySet()) {
			MessageQueue mq = entry.getKey();
			ProcessQueue pq = entry.getValue();

			if (this.removeUnnecessaryMessageQueue(mq, pq)) {
				this.processQueueTable.remove(mq);
				changed = true;
				log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
			}
		}

		boolean allMQLocked = true;
		List<PullRequest> pullRequestList = new ArrayList<>();
		for (MessageQueue mq : mqSet) {
			if (!this.processQueueTable.containsKey(mq)) {
				if (isOrder && !this.lock(mq)) {
					log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
					allMQLocked = true;
					continue;
				}

				this.removeDirtyOffset(mq);
				ProcessQueue pq = createProcessQueue();
				pq.setLocked(true);
				long nextOffset = this.computePullFromWhere(mq);
				if (nextOffset >= 0) {
					ProcessQueue prev = this.processQueueTable.putIfAbsent(mq, pq);
					if (prev != null) {
						log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
					}
					else {
						log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
						PullRequest pullRequest = new PullRequest();
						pullRequest.setConsumerGroup(consumerGroup);
						pullRequest.setNextOffset(nextOffset);
						pullRequest.setMessageQueue(mq);
						pullRequest.setProcessQueue(pq);
						pullRequestList.add(pullRequest);
						changed = true;
					}
				}
				else {
					log.warn("doRebalance, {}, add a new mq failed, {}", consumerGroup, mq);
				}
			}
		}

		if (allMQLocked) {
			mqClientFactory.rebalanceLater(500);
		}

		this.dispatchPullRequest(pullRequestList, 500);

		return changed;
	}

	private boolean updateMessageQueueAssignment(final String topic, final Set<MessageQueueAssignment> assignments, final boolean isOrder) {
		boolean changed = false;
		Map<MessageQueue, MessageQueueAssignment> mq2PushAssignment = new HashMap<>();
		Map<MessageQueue, MessageQueueAssignment> mq2PopAssignment = new HashMap<>();

		for (MessageQueueAssignment assignment : assignments) {
			MessageQueue mq = assignment.getMessageQueue();
			if (mq == null) {
				continue;
			}
			if (assignment.getMode() == MessageRequestMode.POP) {
				mq2PopAssignment.put(mq, assignment);
			}
			else {
				mq2PushAssignment.put(mq, assignment);
			}
		}

		if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
			if (mq2PopAssignment.isEmpty() && !mq2PushAssignment.isEmpty()) {
				try {
					String retryTopic = KeyBuilder.buildPopRetryTopic(topic, getConsumerGroup());
					SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(retryTopic, SubscriptionData.SUB_ALL);
					getSubscriptionInner().put(retryTopic, subscriptionData);
				}
				catch (Exception ignored) {}
			}
			else if (!mq2PopAssignment.isEmpty() && mq2PushAssignment.isEmpty()) {
				try {
					String retryTopic = KeyBuilder.buildPopRetryTopic(topic, getConsumerGroup());
					getSubscriptionInner().remove(retryTopic);
				}
				catch (Exception ignored) {}
			}
		}

		{
			Map<MessageQueue, ProcessQueue> removeQueueMap = new HashMap<>(this.processQueueTable.size());
			Iterator<Map.Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<MessageQueue, ProcessQueue> next = it.next();
				MessageQueue mq = next.getKey();
				ProcessQueue pq = next.getValue();

				if (mq.getTopic().equals(topic)) {
					if (!mq2PushAssignment.containsKey(mq)) {
						pq.setDropped(true);
						removeQueueMap.put(mq, pq);
					}
					else if (pq.isPullExpired() && this.consumeType() == ConsumeType.CONSUME_PASSIVELY) {
						pq.setDropped(true);
						removeQueueMap.put(mq, pq);
						log.error("[BUG]doRebalance, {}, try remove unnecessary mq, {}, because pull is pause, so try to fixed it", consumerGroup, mq);
					}
				}
			}
			for (Map.Entry<MessageQueue, ProcessQueue> entry : removeQueueMap.entrySet()) {
				MessageQueue mq = entry.getKey();
				ProcessQueue pq = entry.getValue();

				if (this.removeUnnecessaryMessageQueue(mq, pq)) {
					this.processQueueTable.remove(mq);
					changed = true;
					log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
				}
			}
		}

		{
			Map<MessageQueue, PopProcessQueue> removeQueueMap = new HashMap<>(this.popProcessQueueTable.size());
			Iterator<Map.Entry<MessageQueue, PopProcessQueue>> it = this.popProcessQueueTable.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<MessageQueue, PopProcessQueue> next = it.next();
				MessageQueue mq = next.getKey();
				PopProcessQueue pq = next.getValue();

				if (mq.getTopic().equals(topic)) {
					if (!mq2PopAssignment.containsKey(mq)) {
						pq.setDropped(true);
						removeQueueMap.put(mq, pq);
					}
					else if (pq.isPullExpired() && this.consumeType() == ConsumeType.CONSUME_PASSIVELY) {
						pq.setDropped(true);
						removeQueueMap.put(mq, pq);
						log.error("[BUG]doRebalance, {}, try remove unnecessary pop mq, {}, but pop is pause, so try to fixed it", consumerGroup, mq);
					}
				}
			}
			for (Map.Entry<MessageQueue, PopProcessQueue> entry : removeQueueMap.entrySet()) {
				MessageQueue mq = entry.getKey();
				PopProcessQueue pq = entry.getValue();

				if (this.removeUnnecessaryMessageQueue(mq, pq)) {
					this.popProcessQueueTable.remove(mq);
					changed = true;
					log.info("doRebalance, {}, remove unnecessary pop mq, {}", consumerGroup, mq);
				}
			}
		}

		{
			boolean allMQLocked = true;
			List<PullRequest> pullRequestList = new ArrayList<>();
			for (MessageQueue mq : mq2PushAssignment.keySet()) {
				if (!this.processQueueTable.containsKey(mq)) {
					if (isOrder && !this.lock(mq)) {
						log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
						allMQLocked = false;
						continue;
					}

					this.removeDirtyOffset(mq);
					ProcessQueue pq = createProcessQueue();
					pq.setLocked(true);

					long nextOffset = -1L;
					try {
						nextOffset = this.computePullFromWhereWithException(mq);
					}
					catch (Exception e) {
						log.warn("doRebalance, {}, compute offset failed, {}", consumerGroup, mq);
						continue;
					}

					if (nextOffset >= 0) {
						ProcessQueue prev = this.processQueueTable.putIfAbsent(mq, pq);
						if (prev != null) {
							log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
						}
						else {
							log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
							PullRequest pullRequest = new PullRequest();
							pullRequest.setConsumerGroup(consumerGroup);
							pullRequest.setNextOffset(nextOffset);
							pullRequest.setMessageQueue(mq);
							pullRequest.setProcessQueue(pq);
							pullRequestList.add(pullRequest);
							changed = true;
						}
					}
					else {
						log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
					}
				}
			}

			if (!allMQLocked) {
				mqClientFactory.rebalanceLater(500);
			}
			this.dispatchPullRequest(pullRequestList, 500);
		}

		{
			List<PopRequest> popRequestList = new ArrayList<>();
			for (MessageQueue mq : mq2PopAssignment.keySet()) {
				if (!this.popProcessQueueTable.containsKey(mq)) {
					PopProcessQueue pq = createPopProcessQueue();
					PopProcessQueue prev = this.popProcessQueueTable.putIfAbsent(mq, pq);
					if (prev != null) {
						log.info("doRebalance, {}, mq pop already exists, {}", consumerGroup, pq);
					}
					else {
						log.info("doRebalance, {}, add a new pop mq, {}", consumerGroup, mq);
						PopRequest popRequest = new PopRequest();
						popRequest.setTopic(topic);
						popRequest.setConsumerGroup(consumerGroup);
						popRequest.setMessageQueue(mq);
						popRequest.setPopProcessQueue(pq);
						popRequest.setInitMode(getConsumeInitMode());
						popRequestList.add(popRequest);
						changed = true;
					}
				}
			}

			this.dispatchPopPullRequest(popRequestList, 500);
		}

		return changed;
	}
}
