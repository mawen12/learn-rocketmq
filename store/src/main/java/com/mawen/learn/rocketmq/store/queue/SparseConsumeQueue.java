package com.mawen.learn.rocketmq.store.queue;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import com.mawen.learn.rocketmq.common.BoundaryType;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.store.MessageStore;
import com.mawen.learn.rocketmq.store.SelectMappedBufferResult;
import com.mawen.learn.rocketmq.store.logfile.MappedFile;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/16
 */
public class SparseConsumeQueue extends BatchConsumeQueue {

	public SparseConsumeQueue(String topic, int queueId, String storePath, int mappedFileSize, MessageStore messageStore) {
		super(topic, queueId, storePath, mappedFileSize, messageStore);
	}

	public SparseConsumeQueue(String topic, int queueId, String storePath, int mappedFileSize, MessageStore messageStore, String subfolder) {
		super(topic, queueId, storePath, mappedFileSize, messageStore, subfolder);
	}

	@Override
	public void recover() {
		CopyOnWriteArrayList<MappedFile> mappedFiles = mappedFileQueue.getMappedFiles();
		if (!mappedFiles.isEmpty()) {
			int index = mappedFiles.size() - 1;
			if (index < 0) {
				index = 0;
			}

			MappedFile mappedFile = mappedFiles.get(index);
			ByteBuffer buffer = mappedFile.sliceByteBuffer();
			int mappedFileOffset = 0;
			long processOffset = mappedFile.getFileFromOffset();

			while (true) {
				for (int i = 0; i < mappedFileSize; i+=CQ_STORE_UNIT_SIZE) {
					buffer.position(1);
					long offset = buffer.getLong();
					int size = buffer.getInt();
					buffer.getLong();
					buffer.getLong();
					long msgBaseOffset = buffer.getLong();
					short batchSize = buffer.getShort();

					if (offset >= 0 && size >= 0 && msgBaseOffset >= 0 && batchSize > 0) {
						mappedFileOffset += CQ_STORE_UNIT_SIZE;
						maxMsgPhyOffsetInCommitLog = offset;
					}
					else {
						log.info("Recover current batch consume queue file over, file:{} offset:{} size:{} msgBaseOffset:{} batchSize:{} mappedFileOffset:{}",
								mappedFile.getFileName(), offset, size, msgBaseOffset, batchSize, mappedFileOffset);

						if (mappedFileOffset != mappedFileSize) {
							mappedFile.setWrotePosition(mappedFileOffset);
							mappedFile.setFlushedPosition(mappedFileOffset);
							mappedFile.setCommittedPosition(mappedFileOffset);
						}

						break;
					}
				}

				index++;
				if (index >= mappedFiles.size()) {
					log.info("Recover last batch consume queue file over, last mapped file:{}", mappedFile.getFileName());
					break;
				}
				else {
					mappedFile = mappedFiles.get(index);
					buffer = mappedFile.sliceByteBuffer();
					processOffset = mappedFile.getFileFromOffset();
					mappedFileOffset = 0;
					log.info("Recover next batch consume queue file:{}", mappedFile.getFileName());
				}
			}

			processOffset += mappedFileOffset;
			mappedFileQueue.setFlushedWhere(processOffset);
			mappedFileQueue.setCommittedWhere(processOffset);
			mappedFileQueue.truncateDirtyFiles(processOffset);
			reviseMaxAndMinOffsetInQueue();
		}
	}

	public ReferredIterator<CqUnit> iterateFromOrNext(long startOffset) {
		SelectMappedBufferResult result = getBatchMsgIndexOrNextBuffer(startOffset);
		return result != null ? new BatchConsumeQueueIterator(result) : null;
	}

	public SelectMappedBufferResult getBatchMsgIndexOrNextBuffer(final long msgOffset) {
		MappedFile targetReq;

		if (msgOffset <= minOffsetInQueue) {
			targetReq = mappedFileQueue.getFirstMappedFile();
		}
		else {
			targetReq = searchFileByOffsetOrRight(msgOffset);
		}

		if (targetReq == null) {
			return null;
		}

		BatchOffsetIndex minOffset = getMinMsgOffset(targetReq, false, false);
		BatchOffsetIndex maxOffset = getMaxMsgOffset(targetReq, false, false);
		if (minOffset == null || maxOffset == minOffset) {
			return null;
		}

		SelectMappedBufferResult result = minOffset.getMappedFile().selectMappedBuffer(0);
		try {
			ByteBuffer buffer = result.getByteBuffer();
			int left = minOffset.getIndexPos();
			int right = maxOffset.getIndexPos();
			int mid = binarySearchRight(buffer, left, right, CQ_STORE_UNIT_SIZE, MSG_BASE_OFFSET_INDEX, msgOffset, BoundaryType.LOWER);
			if (mid != -1) {
				return minOffset.getMappedFile().selectMappedBuffer(mid);
			}
		}
		finally {
			result.release();
		}

		return null;
	}

	protected MappedFile searchOffsetFromCacheOrRight(long msgOffset) {
		Map.Entry<Long, MappedFile> entry = offsetCache.ceilingEntry(msgOffset);
		return entry != null ? entry.getValue() : null;
	}

	protected MappedFile searchFileByOffsetOrRight(long msgOffset) {
		MappedFile targetBcq = null;
		boolean searchBcqByCacheEnable = messageStore.getMessageStoreConfig().isSearchBcqByCacheEnable();
		if (searchBcqByCacheEnable) {
			targetBcq = searchOffsetFromCacheOrRight(msgOffset);
			if (targetBcq == null) {
				MappedFile firstBcq = mappedFileQueue.getFirstMappedFile();
				BatchOffsetIndex minForFirstBcq = getMinMsgOffset(firstBcq, false, false);
				if (minForFirstBcq != null && minForFirstBcq.getMsgOffset() <= msgOffset && msgOffset < maxOffsetInQueue) {
					targetBcq = searchOffsetFromFilesOrRight(msgOffset);
				}
				log.warn("cache is not working on BCQ [Topic: {}, QueueId: {}, for msgOffset: {}, targetBcq: {}]",
						topic, queueId, msgOffset, targetBcq);
			}
		}
		else {
			targetBcq = searchOffsetFromFilesOrRight(msgOffset);
		}

		return targetBcq;
	}

	public MappedFile searchOffsetFromFilesOrRight(long msgOffset) {
		MappedFile targetBcq = null;
		int mappedFileNum = mappedFileQueue.getMappedFiles().size();

		for (int i = mappedFileNum - 1; i >= 0; i--) {
			MappedFile mappedFile = mappedFileQueue.getMappedFiles().get(i);
			BatchOffsetIndex minOffset = getMinMsgOffset(mappedFile, false, false);
			BatchOffsetIndex maxOffset = getMaxMsgOffset(mappedFile, false, false);
			if (maxOffset != null && maxOffset.getMsgOffset() < msgOffset) {
				if (i != mappedFileNum - 1) {
					targetBcq = mappedFileQueue.getMappedFiles().get(i + 1);
					break;
				}
			}

			if (minOffset != null && minOffset.getMsgOffset() <= msgOffset && maxOffset != null && msgOffset <= maxOffset.getMsgOffset()) {
				targetBcq = mappedFile;
				break;
			}
		}

		return targetBcq;
	}

	private MappedFile getPreFile(MappedFile file) {
		int index = mappedFileQueue.getMappedFiles().indexOf(file);
		return index >= 1 ? mappedFileQueue.getMappedFiles().get(index - 1) : null;
	}

	private void cacheOffset(MappedFile file, Function<MappedFile, BatchOffsetIndex> offsetGetFunc) {
		try {
			BatchOffsetIndex offset = offsetGetFunc.apply(file);
			if (offset != null) {
				offsetCache.put(offset.getMsgOffset(), offset.getMappedFile());
				timeCache.put(offset.getStoreTimestamp(), offset.getMappedFile());
			}
		}
		catch (Exception e) {
			log.error("Failed caching offset and time on BCQ [Topic: {}, QueueId: {}, File: {}]", topic, queueId, file);
		}
	}

	@Override
	protected void cacheBcq(MappedFile mappedFile) {
		MappedFile file = getPreFile(mappedFile);
		if (file != null) {
			cacheOffset(file, m -> getMaxMsgOffset(m, false, true));
		}
	}

	public void putEndPositionInfo(MappedFile mappedFile) {
		if (!mappedFile.isFull()) {
			byteBufferItem.flip();
			byteBufferItem.limit(CQ_STORE_UNIT_SIZE);
			byteBufferItem.putLong(-1);
			byteBufferItem.putInt(0);
			byteBufferItem.putLong(0);
			byteBufferItem.putLong(0);
			byteBufferItem.putLong(0);
			byteBufferItem.putShort((short) 0);
			byteBufferItem.putInt(INVALID_POS);
			byteBufferItem.putInt(0);

			boolean appendRes;

			if (messageStore.getMessageStoreConfig().isPutConsumeQueueDataByFileChannel()) {
				appendRes = mappedFile.appendMessageUsingFileChannel(byteBufferItem.array());
			}
			else {
				appendRes = mappedFile.appendMessage(byteBufferItem.array());
			}

			if (!appendRes) {
				log.error("append end position info into {} failed", mappedFile.getFileName());
			}
		}

		cacheOffset(mappedFile, m -> getMaxMsgOffset(m, false,true));
	}

	public MappedFile createFile(final long physicalOffset) {
		return mappedFileQueue.tryCreateMappedFile(physicalOffset);
	}

	public boolean isLastFileFull() {
		if (mappedFileQueue.getLastMappedFile() != null) {
			return mappedFileQueue.getLastMappedFile().isFull();
		}
		return true;
	}

	public boolean shouldRoll() {
		if (mappedFileQueue.getLastMappedFile() == null) {
			return true;
		}
		if (mappedFileQueue.getLastMappedFile().isFull()) {
			return true;
		}
		if (mappedFileQueue.getLastMappedFile().getWrotePosition() + BatchConsumeQueue.CQ_STORE_UNIT_SIZE > mappedFileQueue.getMappedFileSize()) {
			return true;
		}

		return false;
	}

	public boolean containsOffsetFile(final long physicalOffset) {
		String fileName = UtilAll.offset2FileName(physicalOffset);
		return mappedFileQueue.getMappedFiles().stream()
				.anyMatch(mf -> Objects.equals(mf.getFile().getName(), fileName));
	}

	public long getMaxPhyOffsetInLog() {
		MappedFile lastMappedFile = mappedFileQueue.getLastMappedFile();
		Long maxOffsetInLog = getMax(lastMappedFile, b -> b.getLong(0) + b.getInt(8));
		return maxOffsetInLog != null ? maxOffsetInLog : -1;
	}

	private <T> T getMax(MappedFile mappedFile, Function<ByteBuffer, T> function) {
		if (mappedFile == null || mappedFile.getReadPosition() < CQ_STORE_UNIT_SIZE) {
			return null;
		}

		ByteBuffer buffer = mappedFile.sliceByteBuffer();
		for (int i = mappedFile.getReadPosition() - CQ_STORE_UNIT_SIZE; i >= 0; i -= CQ_STORE_UNIT_SIZE) {
			buffer.position(i);
			long offset = buffer.getLong();
			int size = buffer.getInt();
			buffer.getLong();
			buffer.getLong();
			long msgBaseOffset = buffer.getLong();
			short batchSize = buffer.getShort();

			if (offset >= 0 && size >= 0 && msgBaseOffset >= 0 && batchSize >= 0) {
				buffer.position(i);
				return function.apply(buffer.slice());
			}
		}

		return null;
	}

	@Override
	protected BatchOffsetIndex getMaxMsgOffset(MappedFile mappedFile, boolean getBatchSize, boolean getStoreTime) {
		if (mappedFile == null || mappedFile.getReadPosition() < CQ_STORE_UNIT_SIZE) {
			return null;
		}

		ByteBuffer buffer = mappedFile.sliceByteBuffer();
		for (int i = mappedFile.getReadPosition() - CQ_STORE_UNIT_SIZE; i >= 0; i -= CQ_STORE_UNIT_SIZE) {
			buffer.position(i);
			long offset = buffer.getLong();
			int size = buffer.getInt();
			buffer.getLong();
			long timestamp = buffer.getLong();
			long msgBaseOffset = buffer.getLong();
			short batchSize = buffer.getShort();

			if (offset >= 0 && size >= 0 && msgBaseOffset >= 0 && batchSize >= 0) {
				return new BatchOffsetIndex(mappedFile, i, msgBaseOffset, batchSize, timestamp);
			}
		}

		return null;
	}

	public long getMaxMsgOffsetFromFile(String simpleFileName) {
		MappedFile mappedFile = mappedFileQueue.getMappedFiles().stream()
				.filter(m -> Objects.equals(m.getFile().getName(), simpleFileName))
				.findFirst()
				.orElse(null);

		if (mappedFile == null) {
			return -1;
		}

		BatchOffsetIndex max = getMaxMsgOffset(mappedFile, false, false);
		return max != null ? max.getMsgOffset() : -1;
	}

	private void refreshMaxCache() {
		doRefreshCache(m -> getMaxMsgOffset(m, false, true));
	}

	@Override
	protected void refreshCache() {
		refreshMaxCache();
	}

	public void refresh() {
		reviseMaxAndMinOffsetInQueue();
		refreshCache();
	}
}
