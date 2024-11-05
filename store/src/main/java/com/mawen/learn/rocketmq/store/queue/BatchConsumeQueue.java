package com.mawen.learn.rocketmq.store.queue;


import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentSkipListMap;

import com.mawen.learn.rocketmq.common.BoundaryType;
import com.mawen.learn.rocketmq.common.Pair;
import com.mawen.learn.rocketmq.common.attribute.CQType;
import com.mawen.learn.rocketmq.common.filter.MessageFilter;
import com.mawen.learn.rocketmq.common.message.MessageExtBrokerInner;
import com.mawen.learn.rocketmq.store.DispatchRequest;
import com.mawen.learn.rocketmq.store.MappedFileQueue;
import com.mawen.learn.rocketmq.store.MessageStore;
import com.mawen.learn.rocketmq.store.SelectMappedBufferResult;
import com.mawen.learn.rocketmq.store.logfile.MappedFile;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.RocksDBException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/5
 */
@Getter
public class BatchConsumeQueue implements ConsumeQueueInterface {

	private static final Logger log = LoggerFactory.getLogger(BatchConsumeQueue.class);

	/**
	 * <pre>
	 * +---------------------------+-----------+--------------+------------+---------------+-----------+-----------------+----------+
	 * | CommitLog Physical Offset | Body Size | Tag HashCode | Store Time | msgBaseOffset | batchSize | compactedOffset | reversed |
	 * +---------------------------+-----------+--------------+------------+---------------+-----------+-----------------+----------+
	 * | 8 bytes                   | 4 bytes   | 8 bytes      | 8 bytes    | 8 bytes       | 2 bytes   | 4 bytes         | 4 bytes  |
	 * +---------------------------+-----------+--------------+------------+---------------+-----------+-----------------+----------+
	 * </pre>
	 */
	public static final int CQ_STORE_UNIT_SIZE = 46;
	public static final int MSG_TAG_OFFSET_INDEX = 12;
	public static final int MSG_STORE_TIME_OFFSET_INDEX = 20;
	public static final int MSG_BASE_OFFSET_INDEX = 28;
	public static final int MSG_BATCH_SIZE_INDEX = 36;
	public static final int MSG_COMPACT_OFFSET_INDEX = 38;
	public static final int MSG_COMPACT_OFFSET_LENGTH = 4;
	public static final int INVALID_POS = -1;

	protected final MappedFileQueue mappedFileQueue;
	protected MessageStore messageStore;
	protected final String topic;
	protected final int queueId;
	protected final ByteBuffer byteBufferItem;

	protected final String storePath;
	protected final int mappedFileSize;
	protected volatile long maxMsgPhyOffsetInCommitLog = -1;

	protected volatile long minLogicOffset = 0;

	protected volatile long maxOffsetInQueue = 0;
	protected volatile long minOffsetInQueue = -1;
	protected final int commitLogSize;

	protected ConcurrentSkipListMap<Long, MappedFile> offsetCache = new ConcurrentSkipListMap<>();
	protected ConcurrentSkipListMap<Long, MappedFile> timeCache = new ConcurrentSkipListMap<>();

	public BatchConsumeQueue(final String topic, final int queueId, final String storePath, final int mappedFileSize, final MessageStore messageStore) {
		this(topic, queueId, storePath, mappedFileSize, messageStore, StringUtils.EMPTY);
	}

	public BatchConsumeQueue(final String topic, final int queueId, final String storePath, final int mappedFileSize,
			final MessageStore messageStore, final String subfolder) {
		this.storePath = storePath;
		this.mappedFileSize = mappedFileSize;
		this.messageStore = messageStore;
		this.commitLogSize = messageStore.getCommitLog().getCommitLogSize();

		this.topic = topic;
		this.queueId = queueId;

		if (StringUtils.isBlank(subfolder)) {
			String queueDir = storePath + File.separator + topic + File.separator + queueId;
			mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);
		}
		else {
			String queueDir = storePath + File.separator + topic + File.separator + queueId + File.separator + subfolder;
			mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);
		}

		this.byteBufferItem = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
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
		SelectMappedBufferResult result = getBatchMsgIndexBuffer(startIndex);
		return result == null ? null : new BatchConsumeQueueIterator(result);
	}

	@Override
	public ReferredIterator<CqUnit> iterateFrom(long startIndex, int count) throws RocksDBException {
		return iterateFrom(startIndex);
	}

	@Override
	public CqUnit get(long index) {
		ReferredIterator<CqUnit> it = iterateFrom(index);
		return it != null ? it.nextAndRelease() : null;
	}

	@Override
	public Pair<CqUnit, Long> getCqUnitAndStoreTime(long index) {
		CqUnit cqUnit = get(index);
		Long messageStoreTime = messageStore.getQueueStore().getStoreTime(cqUnit);
		return new Pair<>(cqUnit, messageStoreTime);
	}

	@Override
	public Pair<CqUnit, Long> getEarliestUnitAndStoreTime() {
		return null;
	}

	@Override
	public CqUnit getEarliestUnit() {
		return null;
	}

	@Override
	public CqUnit getLatestUnit() {
		return null;
	}

	@Override
	public long getLastOffset() {
		return 0;
	}

	@Override
	public long getMinOffsetInQueue() {
		return 0;
	}

	@Override
	public long getMaxOffsetInQueue() {
		return 0;
	}

	@Override
	public long getMessageTotalInQueue() {
		return 0;
	}

	@Override
	public long getOffsetInQueueByTime(long timestamp) {
		return 0;
	}

	@Override
	public long getOffsetInQueueByTime(long timestamp, BoundaryType boundaryType) {
		return 0;
	}

	@Override
	public long getMaxPhysicOffset() {
		return 0;
	}

	@Override
	public long getMinLogicOffset() {
		return 0;
	}

	@Override
	public CQType getCQType() {
		return null;
	}

	@Override
	public long getTotalSize() {
		return 0;
	}

	@Override
	public long getUnitSize() {
		return 0;
	}

	@Override
	public void correctMinOffset(long minCommitLogOffset) {

	}

	@Override
	public void putMessagePositionInfoWrapper(DispatchRequest request) {

	}

	@Override
	public void assignQueueOffset(QueueOffsetOperator queueOffsetOperator, MessageExtBrokerInner msg) throws RocksDBException {

	}

	@Override
	public void increaseQueueOffset(QueueOffsetOperator queueOffsetOperator, MessageExtBrokerInner msg, short messageNum) {

	}

	@Override
	public void estimateMessageCount(long from, long to, MessageFilter filter) {

	}

	@Override
	public boolean load() {
		return false;
	}

	@Override
	public void recover() {

	}

	@Override
	public void checkSelf() {

	}

	@Override
	public boolean flush(int flushLeastPages) {
		return false;
	}

	@Override
	public void destroy() {

	}

	@Override
	public void truncateDirtyLogicFiles(long maxCommitLogPos) {

	}

	@Override
	public int deleteExpiredFile(long minCommitLogPos) {
		return 0;
	}

	@Override
	public long rollNextFile(long nextBeginOffset) {
		return 0;
	}

	@Override
	public boolean isFirstFileAvailable() {
		return false;
	}

	@Override
	public boolean isFirstFileExist() {
		return false;
	}

	@Override
	public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {

	}

	@Override
	public void cleanSwappedMap(long forceCleanSwapIntervalMs) {

	}

	static class BatchConsumeQueueIterator implements ReferredIterator<CqUnit> {

		private SelectMappedBufferResult sbr;
		private int relativePos;

		public BatchConsumeQueueIterator(SelectMappedBufferResult sbr) {
			this.sbr = sbr;
			if (sbr != null && sbr.getByteBuffer() != null) {
				relativePos = sbr.getByteBuffer().position();
			}
		}

		@Override
		public void release() {
			if (sbr != null) {
				sbr.release();
				sbr = null;
			}
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
			return sbr != null && sbr.getByteBuffer() != null && sbr.getByteBuffer().hasRemaining();
		}

		@Override
		public CqUnit next() {
			if (!hasNext()) {
				return null;
			}

			ByteBuffer tmpBuffer = sbr.getByteBuffer().slice();
			tmpBuffer.position(MSG_COMPACT_OFFSET_INDEX);
			ByteBuffer compactOffsetStoreBuffer = tmpBuffer.slice();
			compactOffsetStoreBuffer.limit(MSG_COMPACT_OFFSET_LENGTH);

			int relativePos = sbr.getByteBuffer().position();
			long offsetPy = sbr.getByteBuffer().getLong();
			int sizePy = sbr.getByteBuffer().getInt();
			long tagsCode = sbr.getByteBuffer().getLong();
			sbr.getByteBuffer().getLong();
			long msgBaseOffset = sbr.getByteBuffer().getLong();
			short batchSize = sbr.getByteBuffer().getShort();
			int compactedOffset = sbr.getByteBuffer().getInt();
			sbr.getByteBuffer().position(relativePos + CQ_STORE_UNIT_SIZE);

			return new CqUnit(msgBaseOffset, offsetPy, sizePy, tagsCode, batchSize, compactedOffset, compactOffsetStoreBuffer);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("remove");
		}
	}
}
