package com.mawen.learn.rocketmq.store.queue;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.mawen.learn.rocketmq.common.BoundaryType;
import com.mawen.learn.rocketmq.common.Pair;
import com.mawen.learn.rocketmq.common.attribute.CQType;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.filter.MessageFilter;
import com.mawen.learn.rocketmq.common.message.MessageExtBrokerInner;
import com.mawen.learn.rocketmq.store.ConsumeQueue;
import com.mawen.learn.rocketmq.store.DispatchRequest;
import com.mawen.learn.rocketmq.store.MessageStore;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.RocksDBException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/7
 */
@AllArgsConstructor
public class RocksDBConsumeQueue implements ConsumeQueueInterface {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
	private static final Logger ERROR_LOG = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

	private final MessageStore messageStore;
	private final String topic;
	private final int queueId;

	public RocksDBConsumeQueue(String topic, int queueId) {
		this.messageStore = null;
		this.topic = topic;
		this.queueId = queueId;
	}

	@Override
	public String getTopic() {
		return topic;
	}

	@Override
	public int getQueueId() {
		return queueId;
	}

	@Override
	public ReferredIterator<CqUnit> iterateFrom(long startIndex) {
		try {
			long maxCqOffset = getMaxOffsetInQueue();
			if (startIndex < maxCqOffset) {
				int num = pullNum(startIndex, maxCqOffset);
				return iterateFrom0(startIndex, num);
			}
		}
		catch (RocksDBException e) {
			log.error("[RocksDBConsumeQueue] iterateFrom error!", e);
		}
		return null;
	}

	private int pullNum(long cqOffset, long maxCqOffset) {
		long diffLong = maxCqOffset - cqOffset;
		if (diffLong < Integer.MAX_VALUE) {
			int diffInt = (int) diffLong;
			return diffInt > 16 ? 16 : diffInt;
		}
		return 16;
	}

	@Override
	public ReferredIterator<CqUnit> iterateFrom(long startIndex, int count) throws RocksDBException {
		long maxCqOffset = getMaxOffsetInQueue();
		if (startIndex < maxCqOffset) {
			int num = Math.min((int)(maxCqOffset - startIndex), count);
			return iterateFrom0(startIndex, num);
		}
		return null;
	}

	private ReferredIterator<CqUnit> iterateFrom0(final long startIndex, final int count) throws RocksDBException {
		List<ByteBuffer> bufferList = messageStore.getQueueStore().rangeQuery(topic, queueId, startIndex, count);
		if (CollectionUtils.isEmpty(bufferList)) {
			if (messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
				log.warn("iterateFrom0 - find nothing, startIndex: {}, count: {}", startIndex, count);
			}
			return null;
		}

		return new RocksDBConsumeQueueIterator(bufferList, startIndex);
	}

	@Override
	public CqUnit get(long index) {
		Pair<CqUnit, Long> pair = getCqUnitAndStoreTime(index);
		return pair != null ? pair.getObject1() : null;
	}

	@Override
	public Pair<CqUnit, Long> getCqUnitAndStoreTime(long index) {
		ByteBuffer buffer;
		try {
			buffer = messageStore.getQueueStore().get(topic, queueId, index);
			long phyOffset = buffer.getLong();
			int size = buffer.getInt();
			long tagCode = buffer.getLong();
			long messageStoreTime = buffer.getLong();
			return new Pair<>(new CqUnit(index, phyOffset, size, tagCode), messageStoreTime);
		}
		catch (RocksDBException e) {
			ERROR_LOG.error("getUnitAndStoreTime Failed. topic: {}, queueId: {}", topic, queueId, e);
		}
		return null;
	}

	@Override
	public Pair<CqUnit, Long> getEarliestUnitAndStoreTime() {
		try {
			long minOffset = messageStore.getQueueStore().getMinOffsetInQueue(topic, queueId);
			return getCqUnitAndStoreTime(minOffset);
		}
		catch (RocksDBException e) {
			ERROR_LOG.error("getEarliestUnitAndStoreTime Failed. topic: {}, queueId: {}", topic, queueId, e);
		}
		return null;
	}

	@Override
	public CqUnit getEarliestUnit() {
		Pair<CqUnit, Long> pair = getEarliestUnitAndStoreTime();
		return pair != null ? pair.getObject1() : null;
	}

	@Override
	public CqUnit getLatestUnit() {
		try {
			long maxOffset = messageStore.getQueueStore().getMaxOffsetInQueue(topic, queueId);
			return get(maxOffset);
		}
		catch (RocksDBException e) {
			ERROR_LOG.error("getLatestUnit Failed. topic: {}, queueId: {} {}", topic, queueId, e.getMessage());
		}
		return null;
	}

	@Override
	public long getLastOffset() {
		return getMaxPhysicOffset();
	}

	@Override
	public long getMinOffsetInQueue() {
		return messageStore.getMinOffsetInQueue(topic ,queueId);
	}

	@Override
	public long getMaxOffsetInQueue() {
		try {
			return messageStore.getQueueStore().getMaxOffsetInQueue(topic, queueId);
		}
		catch (RocksDBException e) {
			ERROR_LOG.error("getMaxOffsetInQueue Failed. topic: {}, queueId: {}", topic, queueId, e);
		}
		return 0;
	}

	@Override
	public long getMessageTotalInQueue() {
		try {
			long maxOffsetInQueue = messageStore.getQueueStore().getMaxOffsetInQueue(topic, queueId);
			long minOffsetInQueue = messageStore.getQueueStore().getMinOffsetInQueue(topic, queueId);
			return maxOffsetInQueue - minOffsetInQueue;
		}
		catch (RocksDBException e) {
			ERROR_LOG.error("getMessageTotalInQueue Failed. topic: {}, queueId: {}", topic ,queueId, e);
		}
		return 0;
	}

	@Override
	public long getOffsetInQueueByTime(long timestamp) {
		// NOP
		return 0;
	}

	@Override
	public long getOffsetInQueueByTime(long timestamp, BoundaryType boundaryType) {
		// NOP
		return 0;
	}

	@Override
	public long getMaxPhysicOffset() {
		Long maxPhyOffset = messageStore.getQueueStore().getMaxPhyOffsetInConsumeQueue(topic, queueId);
		return maxPhyOffset == null ? -1 : maxPhyOffset;
	}

	@Override
	public long getMinLogicOffset() {
		// NOP
		return 0;
	}

	@Override
	public CQType getCQType() {
		return CQType.RocksDBCQ;
	}

	@Override
	public long getTotalSize() {
		// NOP
		return 0;
	}

	@Override
	public long getUnitSize() {
		return ConsumeQueue.CQ_STORE_UNIT_SIZE;
	}

	@Override
	public void correctMinOffset(long minCommitLogOffset) {
		// NOP
	}

	@Override
	public void putMessagePositionInfoWrapper(DispatchRequest request) {
		// NOP
	}

	@Override
	public void assignQueueOffset(QueueOffsetOperator queueOffsetOperator, MessageExtBrokerInner msg) throws RocksDBException {
		String topicQueueKey = getTopic() + "-" + getQueueId();
		Long queueOffset = queueOffsetOperator.getTopicQueueNextOffset(topicQueueKey);
		if (queueOffset == null) {
			queueOffset = messageStore.getQueueStore().getMaxOffsetInQueue(topic, queueId);
			queueOffsetOperator.updateQueueOffset(topicQueueKey, queueOffset);
		}
		msg.setQueueOffset(queueOffset);
	}

	@Override
	public void increaseQueueOffset(QueueOffsetOperator queueOffsetOperator, MessageExtBrokerInner msg, short messageNum) {
		String topicQueueKey = getTopic() + "-" + getQueueId();
		queueOffsetOperator.increaseQueueOffset(topicQueueKey, messageNum);
	}

	@Override
	public long estimateMessageCount(long from, long to, MessageFilter filter) {
		// NOP
		return 0;
	}

	@Override
	public boolean load() {
		return true;
	}

	@Override
	public void recover() {
		// NOP
	}

	@Override
	public void checkSelf() {
		// NOP
	}

	@Override
	public boolean flush(int flushLeastPages) {
		return true;
	}

	@Override
	public void destroy() {
		// NOP
	}

	@Override
	public void truncateDirtyLogicFiles(long maxCommitLogPos) {
		// NOP
	}

	@Override
	public int deleteExpiredFile(long minCommitLogPos) {
		// NOP
		return 0;
	}

	@Override
	public long rollNextFile(long nextBeginOffset) {
		// NOP
		return 0;
	}

	@Override
	public boolean isFirstFileAvailable() {
		return true;
	}

	@Override
	public boolean isFirstFileExist() {
		return true;
	}

	@Override
	public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {
		// NOP
	}

	@Override
	public void cleanSwappedMap(long forceCleanSwapIntervalMs) {
		// NOP
	}

	private class RocksDBConsumeQueueIterator implements ReferredIterator<CqUnit> {
		private final List<ByteBuffer> bufferList;
		private final long startIndex;
		private final int totalCount;
		private int currentIndex;

		public RocksDBConsumeQueueIterator(List<ByteBuffer> bufferList, long startIndex) {
			this.bufferList = bufferList;
			this.startIndex = startIndex;
			this.totalCount = bufferList.size();
			this.currentIndex = 0;
		}

		@Override
		public void release() {
			// NOP
		}

		@Override
		public CqUnit nextAndRelease() {
			try {
				return next();
			}
			finally {
				release();
			}
		}

		@Override
		public boolean hasNext() {
			return currentIndex < totalCount;
		}

		@Override
		public CqUnit next() {
			if (!hasNext()) {
				return null;
			}

			ByteBuffer buffer = bufferList.get(currentIndex++);
			return new CqUnit(startIndex + currentIndex, buffer.getLong(), buffer.getInt(), buffer.getLong());
		}
	}
}
