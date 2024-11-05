package com.mawen.learn.rocketmq.store.queue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.utils.ConcurrentHashMapUtils;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;


/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/5
 */
@Setter
@Getter
public class QueueOffsetOperator {

	private static final Logger log = LoggerFactory.getLogger(QueueOffsetOperator.class);

	private ConcurrentMap<String, Long> topicQueueTable = new ConcurrentHashMap<>(1024);
	private ConcurrentMap<String, Long> batchTopicQueueTable = new ConcurrentHashMap<>(1024);
	private ConcurrentMap<String, Long> lmqTopicQueueTable = new ConcurrentHashMap<>(1024);

	public long getQueueOffset(String topicQueueKey) {
		return ConcurrentHashMapUtils.computeIfAbsent(topicQueueTable, topicQueueKey, k -> 0L);
	}

	public Long getTopicQueueNextOffset(String topicQueueKey) {
		return topicQueueTable.get(topicQueueKey);
	}

	public void increaseQueueOffset(String topicQueueKey, short messageNum) {
		long queueOffset = getQueueOffset(topicQueueKey);
		topicQueueTable.put(topicQueueKey, queueOffset + messageNum);
	}

	public void updateQueueOffset(String topicQueueKey, long offset) {
		topicQueueTable.put(topicQueueKey, offset);
	}

	public long getBatchQueueOffset(String topicQueueKey) {
		return ConcurrentHashMapUtils.computeIfAbsent(batchTopicQueueTable, topicQueueKey, k -> 0L);
	}

	public void increaseBatchQueueOffset(String topicQueueKey, short messageNum) {
		long batchQueueOffset = getBatchQueueOffset(topicQueueKey);
		batchTopicQueueTable.put(topicQueueKey, batchQueueOffset + messageNum);
	}

	public long getLmqOffset(String topicQueueKey) {
		return ConcurrentHashMapUtils.computeIfAbsent(lmqTopicQueueTable, topicQueueKey, k -> 0L);
	}

	public Long getLmqTopicQueueNextOffset(String topicQueueKey) {
		return lmqTopicQueueTable.get(topicQueueKey);
	}

	public void increaseLmqOffset(String queueKey, short messageNum) {
		long lmqOffset = getLmqOffset(queueKey);
		lmqTopicQueueTable.put(queueKey, lmqOffset + messageNum);
	}

	public long currentQueueOffset(String topicQueueKey) {
		return topicQueueTable.getOrDefault(topicQueueKey, 0L);
	}

	public synchronized void remove(String topic, Integer queueId) {
		String topicQueueKey = topic + "-" + queueId;
		topicQueueTable.remove(topicQueueKey);
		batchTopicQueueTable.remove(topicQueueKey);
		lmqTopicQueueTable.remove(topicQueueKey);

		log.info("removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}", topic, queueId);
	}

	public void setLmqTopicQueueTable(ConcurrentMap<String, Long> lmqTopicQueueTable) {
		ConcurrentMap<String, Long> table = new ConcurrentHashMap<>(1024);
		for (Map.Entry<String, Long> entry : lmqTopicQueueTable.entrySet()) {
			if (MixAll.isLmq(entry.getKey())) {
				table.put(entry.getKey(), entry.getValue());
			}
		}
		lmqTopicQueueTable = table;
	}
}
