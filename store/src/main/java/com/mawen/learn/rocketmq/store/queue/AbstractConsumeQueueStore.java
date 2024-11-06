package com.mawen.learn.rocketmq.store.queue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.message.MessageExtBrokerInner;
import com.mawen.learn.rocketmq.store.DefaultMessageStore;
import com.mawen.learn.rocketmq.store.DispatchRequest;
import com.mawen.learn.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.RocksDBException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/6
 */
public abstract class AbstractConsumeQueueStore implements ConsumeQueueStoreInterface {
	protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	protected final DefaultMessageStore messageStore;
	protected final MessageStoreConfig messageStoreConfig;
	protected final QueueOffsetOperator queueOffsetOperator = new QueueOffsetOperator();
	protected final ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> consumeQueueTable;

	public AbstractConsumeQueueStore(DefaultMessageStore messageStore) {
		this.messageStore = messageStore;
		this.messageStoreConfig = messageStore.getMessageStoreConfig();
		this.consumeQueueTable = new ConcurrentHashMap<>(32);
	}

	@Override
	public void putMessagePositionInfoWrapper(ConsumeQueueInterface consumeQueue, DispatchRequest request) {
		consumeQueue.putMessagePositionInfoWrapper(request);
	}

	@Override
	public Long getMaxOffset(String topic, int queueId) {
		return queueOffsetOperator.currentQueueOffset(topic + "-" + queueId);
	}

	@Override
	public void setTopicQueueTable(ConcurrentMap<String, Long> topicQueueTable) {
		queueOffsetOperator.setTopicQueueTable(topicQueueTable);
		queueOffsetOperator.setLmqTopicQueueTable(topicQueueTable);
	}

	@Override
	public ConcurrentMap getTopicQueueTable() {
		return queueOffsetOperator.getTopicQueueTable();
	}

	@Override
	public void assignQueueOffset(MessageExtBrokerInner msg) throws RocksDBException {
		ConsumeQueueInterface consumeQueue = findOrCreateConsumeQueue(msg.getTopic(), msg.getQueueId());
		consumeQueue.assignQueueOffset(queueOffsetOperator, msg);
	}

	@Override
	public void increaseQueueOffset(MessageExtBrokerInner msg, short messageNum) {
		ConsumeQueueInterface consumeQueue = findOrCreateConsumeQueue(msg.getTopic(), msg.getQueueId());
		consumeQueue.increaseQueueOffset(queueOffsetOperator, msg, messageNum);
	}

	@Override
	public void increaseLmqOffset(String queueKey, short messageNum) {
		queueOffsetOperator.increaseLmqOffset(queueKey, messageNum);
	}

	@Override
	public long getLmqQueueOffset(String queueKey) {
		return queueOffsetOperator.getLmqOffset(queueKey);
	}

	@Override
	public void removeTopicQueueTable(String topic, Integer queueId) {
		queueOffsetOperator.remove(topic, queueId);
	}

	@Override
	public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> getConsumeQueueTable() {
		return consumeQueueTable;
	}

	@Override
	public ConcurrentMap<Integer, ConsumeQueueInterface> findConsumeQueueMap(String topic) {
		return consumeQueueTable.get(topic);
	}

	@Override
	public long getStoreTime(CqUnit cqUnit) {
		if (cqUnit != null) {
			try {
				long phyOffset = cqUnit.getPos();
				int size = cqUnit.getSize();
				return messageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
			}
			catch (Exception ignored) {}
		}
		return -1;
	}
}
