package com.mawen.learn.rocketmq.client.consumer.store;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mawen.learn.rocketmq.client.exception.MQBrokerException;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.exception.OffsetNotFoundException;
import com.mawen.learn.rocketmq.client.impl.FindBrokerResult;
import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.exception.RemotingException;
import com.mawen.learn.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import lombok.RequiredArgsConstructor;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/23
 */
@RequiredArgsConstructor
public class RemoteBrokerOffsetStore implements OffsetStore {
	private static final Logger log = LoggerFactory.getLogger(RemoteBrokerOffsetStore.class);

	private final MQClientInstance mqClientFactory;
	private final String groupName;
	private ConcurrentMap<MessageQueue, ControllableOffset> offsetTable = new ConcurrentHashMap<>();


	@Override
	public void load() throws MQClientException {
		// NOP
	}

	@Override
	public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
		if (mq != null) {
			ControllableOffset offsetOld = this.offsetTable.get(mq);
			if (offsetOld == null) {
				offsetOld = this.offsetTable.putIfAbsent(mq, new ControllableOffset(offset));
			}

			if (offsetOld != null) {
				offsetOld.update(offset, increaseOnly);
			}
		}
	}

	@Override
	public void updateAndFreezeOffset(MessageQueue mq, long offset) {
		if (mq != null) {
			this.offsetTable.computeIfAbsent(mq, k -> new ControllableOffset(offset))
					.updateAndFreeze(offset);
		}
	}

	@Override
	public long readOffset(MessageQueue mq, ReadOffsetType type) {
		if (mq != null) {
			switch (type) {
				case MEMORY_FIRST_THEN_STORE:
				case READ_FROM_MEMORY: {
					ControllableOffset offset = this.offsetTable.get(mq);
					if (offset != null) {
						return offset.getOffset();
					}
					else if (ReadOffsetType.READ_FROM_MEMORY == type) {
						return -1;
					}
				}
				case READ_FROM_STORE:
					try {
						long brokerOffset = this.fetchConsumeOffsetFromBroker(mq);
						this.updateOffset(mq, brokerOffset, false);
						return brokerOffset;
					}
					catch (OffsetNotFoundException e) {
						return -1;
					}
					catch (Exception e) {
						log.warn("fetchConsumeOffsetFromBroker exception, {}", mq, e);
						return -2;
					}
				default:
					break;
			}
		}


		return -3;
	}

	@Override
	public void removeOffset(MessageQueue mq) {
		if (mq != null) {
			this.offsetTable.remove(mq);
			log.info("remove unnecessary messageQueue offset. group={}, mq={}, offsetTableSize={}", this.groupName, mq, offsetTable.size());
		}
	}

	@Override
	public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
		Map<MessageQueue, Long> cloneOffsetTable = new HashMap<>(this.offsetTable.size(), 1);

		for (Map.Entry<MessageQueue, ControllableOffset> entry : this.offsetTable.entrySet()) {
			MessageQueue mq = entry.getKey();
			if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
				continue;
			}

			cloneOffsetTable.put(mq, entry.getValue().getOffset());
		}

		return cloneOffsetTable;
	}

	@Override
	public void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
		String brokerAddr = getBrokerAddrFromMessageQueue(mq, false);

		if (brokerAddr != null) {
			UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
			requestHeader.setTopic(mq.getTopic());
			requestHeader.setConsumerGroup(this.groupName);
			requestHeader.setQueueId(mq.getQueueId());
			requestHeader.setCommitOffset(offset);
			requestHeader.setBrokerName(mq.getBrokerName());

			if (isOneway) {
				this.mqClientFactory.getMQClientAPIImpl().updateConsumerOffsetOnway(brokerAddr, requestHeader, 5 * 1000);
			}
			else {
				this.mqClientFactory.getMQClientAPIImpl().updateConsumerOffset(brokerAddr, requestHeader, 5 * 1000);
			}
		}

		throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
	}

	@Override
	public void persist(MessageQueue mq) {
		ControllableOffset offset = offsetTable.get(mq);
		if (offset != null) {
			try {
				this.updateConsumeOffsetToBroker(mq, offset.getOffset());
				log.info("[persist] group: {}, ClientId: {}, updateConsumeOffsetToBroker: {} {}", this.groupName, this.mqClientFactory.getClientId(), mq, offset.getOffset());
			}
			catch (Exception e) {
				log.error("updateConsumeOffsetToBroker exception", e);
			}
		}
	}

	@Override
	public void persistAll(Set<MessageQueue> mqs) {
		if (mqs == null || mqs.isEmpty()) {
			return;
		}

		final Set<MessageQueue> unusedMQ = new HashSet<>();

		for (Map.Entry<MessageQueue, ControllableOffset> entry : this.offsetTable.entrySet()) {
			MessageQueue mq = entry.getKey();
			ControllableOffset offset = entry.getValue();

			if (offset != null) {
				if (mqs.contains(mq)) {
					try {
						this.updateConsumeOffsetToBroker(mq, offset.getOffset());
						log.info("[persistAll] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}", groupName, mqClientFactory.getClientId(), mq, offset.getOffset());
					}
					catch (Exception e) {
						log.error("updateConsumeOffsetToBroker exception, {}", mq, e);
					}
				}
				else {
					unusedMQ.add(mq);
				}
			}
		}

		if (!unusedMQ.isEmpty()) {
			for (MessageQueue mq : unusedMQ) {
				this.offsetTable.remove(mq);
				log.info("remove unused mq, {}, {}", mq, groupName);
			}
		}
	}

	private void updateConsumeOffsetToBroker(MessageQueue mq, long offset) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
		updateConsumeOffsetToBroker(mq, offset, true);
	}

	private long fetchConsumeOffsetFromBroker(MessageQueue mq) throws MQClientException, MQBrokerException {
		String brokerAddr = getBrokerAddrFromMessageQueue(mq, true);

		if (brokerAddr != null) {
			QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
			requestHeader.setTopic(mq.getTopic());
			requestHeader.setConsumerGroup(this.groupName);
			requestHeader.setQueueId(mq.getQueueId());
			requestHeader.setBrokerName(mq.getBrokerName());

			return this.mqClientFactory.getMQClientAPIImpl().queryConsumerOffset(brokerAddr, requestHeader, 5 * 1000);
		}

		throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
	}

	private String getBrokerAddrFromMessageQueue(MessageQueue mq, boolean onlyThisBroker) {
		FindBrokerResult findBrokerResult = this.mqClientFactory.findBrokerAddressInSubscribe(this.mqClientFactory.getBrokerNameFromMessageQueue(mq), MixAll.MASTER_ID, onlyThisBroker);
		if (findBrokerResult == null) {
			this.mqClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
			findBrokerResult = this.mqClientFactory.findBrokerAddressInSubscribe(this.mqClientFactory.getBrokerNameFromMessageQueue(mq), MixAll.MASTER_ID, onlyThisBroker);
		}

		return findBrokerResult != null ? findBrokerResult.getBrokerAddr() : null;
	}
}
