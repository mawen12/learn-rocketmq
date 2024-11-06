package com.mawen.learn.rocketmq.store.queue;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import com.mawen.learn.rocketmq.common.BoundaryType;
import com.mawen.learn.rocketmq.common.message.MessageExtBrokerInner;
import com.mawen.learn.rocketmq.store.DispatchRequest;
import org.rocksdb.RocksDBException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/6
 */
public interface ConsumeQueueStoreInterface {

	void start();

	boolean load();

	boolean loadAfterDestroy();

	void recover();

	boolean recoverConcurrently();

	boolean shutdown();

	void destroy();

	void destroy(ConsumeQueueInterface consumeQueue) throws RocksDBException;

	boolean flush(ConsumeQueueInterface consumeQueue, int flushLeastPages);

	void cleanExpired(long minPhyOffset);

	void checkSelf();

	int deleteExpiredFile(ConsumeQueueInterface consumeQueue, long minCommitLogPos);

	boolean isFirstFileAvailable(ConsumeQueueInterface consumeQueue);

	boolean isFirstFileExist(ConsumeQueueInterface consumeQueue);

	long rollNextFile(ConsumeQueueInterface consumeQueue, final long offset);

	void truncateDirty(long offsetToTruncate) throws RocksDBException;

	void putMessagePositionInfoWrapper(ConsumeQueueInterface consumeQueue, DispatchRequest request);

	void putMessagePositionInfoWrapper(DispatchRequest request) throws RocksDBException;

	List<ByteBuffer> rangeQuery(final String topic, final int queueId, final long startIndex, final int num) throws RocksDBException;

	ByteBuffer get(final String topic, final int queueId, final long startIndex) throws RocksDBException;

	ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> getConsumeQueueTable();

	void assignQueueOffset(MessageExtBrokerInner msg) throws RocksDBException;

	void increaseQueueOffset(MessageExtBrokerInner msg, short messageNum);

	void increaseLmqOffset(String queueKey, short messageNum);

	long getLmqQueueOffset(String queueKey);

	void recoverOffsetTable(long minPhyOffset);

	void setTopicQueueTable(ConcurrentMap<String, Long> topicQueueTable);

	void removeTopicQueueTable(String topic, Integer queueId);

	ConcurrentMap getTopicQueueTable();

	Long getMaxPhyOffsetInConsumeQueue(String topic, int queueId);

	Long getMaxOffset(String topic, int queueId);

	long getMaxPhyOffsetInConsumeQueue() throws RocksDBException;

	long getMinOffsetInQueue(final String topic, int queueId) throws RocksDBException;

	long getMaxOffsetInQueue(final String topic, final int queueId) throws RocksDBException;

	long getOffsetInQueueByTime(String topic, int queueId, long timestamp, BoundaryType boundaryType) throws RocksDBException;

	ConsumeQueueInterface findOrCreateConsumeQueue(String topic, int queueId);

	ConcurrentMap<Integer, ConsumeQueueInterface> findConsumeQueueMap(String topic);

	long getTotalSize();

	long getStoreTime(CqUnit cqUnit);
}
