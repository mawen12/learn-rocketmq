package com.mawen.learn.rocketmq.store.queue;

import com.mawen.learn.rocketmq.common.BoundaryType;
import com.mawen.learn.rocketmq.common.Pair;
import com.mawen.learn.rocketmq.common.attribute.CQType;
import com.mawen.learn.rocketmq.common.filter.MessageFilter;
import com.mawen.learn.rocketmq.common.message.MessageExtBrokerInner;
import org.rocksdb.RocksDBException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
public interface ConsumeQueueInterface extends FileQueueLifeCycle {

	String getTopic();

	int getQueueId();

	ReferredIterator<CqUnit> iterateFrom(long startIndex);

	ReferredIterator<CqUnit> iterateFrom(long startIndex, int count) throws RocksDBException;

	CqUnit get(long index);

	Pair<CqUnit, Long> getCqUnitAndStoreTime(long index);

	Pair<CqUnit, Long> getEarliestUnitAndStoreTime();

	CqUnit getEarliestUnit();

	CqUnit getLatestUnit();

	long getLastOffset();

	long getMinOffsetInQueue();

	long getMaxOffsetInQueue();

	long getMessageTotalInQueue();

	long getOffsetInQueueByTime(final long timestamp);

	long getOffsetInQueueByTime(final long timestamp, final BoundaryType boundaryType);

	long getMaxPhysicOffset();

	long getMinLogicOffset();

	CQType getCQType();

	long getTotalSize();

	long getUnitSize();

	void correctMinOffset(long minCommitLogOffset);

	void putMessagePositionInfoWrapper(DispatchRequest request);

	void assignQueueOffset(QueueOffsetOperator queueOffsetOperator, MessageExtBrokerInner msg) throws RocksDBException;

	void increaseQueueOffset(QueueOffsetOperator queueOffsetOperator, MessageExtBrokerInner msg, short messageNum);

	void estimateMessageCount(long from, long to, MessageFilter filter);
}
