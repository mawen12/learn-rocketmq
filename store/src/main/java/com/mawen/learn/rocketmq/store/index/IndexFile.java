package com.mawen.learn.rocketmq.store.index;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.List;

import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.store.logfile.DefaultMappedFile;
import com.mawen.learn.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/17
 */
public class IndexFile {
	private static final Logger log = LoggerFactory.getLogger(IndexFile.class);

	private static int hashSlotSize = 4;

	private static int indexSize = 20;
	private static int invalidIndex = 0;

	private final int hashSlotNum;
	private final int indexNum;
	private final int fileTotalSize;
	private final MappedFile mappedFile;
	private final MappedByteBuffer mappedByteBuffer;
	private final IndexHeader indexHeader;

	public IndexFile(String fileName, int hashSlotNum, int indexNum, long endPhyOffset, long endTimestamp) throws IOException {
		this.fileTotalSize = IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
		this.mappedFile = new DefaultMappedFile(fileName, fileTotalSize);
		this.mappedByteBuffer = mappedFile.getMappedByteBuffer();
		this.hashSlotNum = hashSlotNum;
		this.indexNum = indexNum;

		MappedByteBuffer byteBuffer = mappedByteBuffer.slice();
		this.indexHeader = new IndexHeader(byteBuffer);

		if (endPhyOffset > 0) {
			this.indexHeader.setBeginPhyOffset(endPhyOffset);
			this.indexHeader.setEndPhyOffset(endPhyOffset);
		}

		if (endTimestamp > 0) {
			this.indexHeader.setBeginTimestamp(endTimestamp);
			this.indexHeader.setEndTimestamp(endTimestamp);
		}
	}

	public String getFileName() {
		return mappedFile.getFileName();
	}

	public int getFileSize() {
		return fileTotalSize;
	}

	public void load() {
		indexHeader.load();
	}

	public void shutdown() {
		flush();
		UtilAll.cleanBuffer(mappedByteBuffer);
	}

	public void flush() {
		long begin = System.currentTimeMillis();
		if (mappedFile.hold()) {
			indexHeader.updateByteBuffer();
			mappedByteBuffer.force();
			mappedFile.release();
			log.info("flush index file elapsed time(ms) {}", System.currentTimeMillis() - begin);
		}
	}

	public boolean isWriteFull() {
		return indexHeader.getIndexCount() >= indexNum;
	}

	public boolean destroy(final long intervalForcibly) {
		return mappedFile.destroy(intervalForcibly);
	}

	public boolean putKey(String key, long phyOffset, long storeTimestamp) {
		if (indexHeader.getIndexCount() < indexNum) {
			int keyHash = indexKeyHashMethod(key);
			int slotPos = keyHash % hashSlotNum;
			int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

			try {
				int slotValue = mappedByteBuffer.getInt(absSlotPos);
				if (slotValue <= invalidIndex || slotValue > indexHeader.getIndexCount()) {
					slotValue = invalidIndex;
				}

				long timeDiff = storeTimestamp - indexHeader.getBeginTimestamp();
				timeDiff = timeDiff / 1000;

				if (indexHeader.getBeginTimestamp() <= 0) {
					timeDiff = 0;
				}
				else if (timeDiff > Integer.MAX_VALUE) {
					timeDiff = Integer.MAX_VALUE;
				}
				else if (timeDiff < 0) {
					timeDiff = 0;
				}

				int absIndexPos = IndexHeader.INDEX_HEADER_SIZE + hashSlotNum * hashSlotSize + indexHeader.getIndexCount() * indexSize;

				mappedByteBuffer.putInt(absIndexPos, keyHash);
				mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
				mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
				mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);
				mappedByteBuffer.putInt(absIndexPos, indexHeader.getIndexCount());

				if (indexHeader.getIndexCount() <= 1) {
					indexHeader.setBeginPhyOffset(phyOffset);
					indexHeader.setBeginTimestamp(storeTimestamp);
				}

				if (invalidIndex == slotValue) {
					indexHeader.incHashSlotCount();
				}
				indexHeader.incIndexCount();
				indexHeader.setEndPhyOffset(phyOffset);
				indexHeader.setEndTimestamp(storeTimestamp);

				return true;
			}
			catch (Exception e) {
				log.error("putKey exception, key: {} KeyHashCode: {}", key, key.hashCode(), e);
			}
		}
		else {
			log.warn("Over index file capacity: index count={}; index max num={}", indexHeader.getIndexCount(), indexNum);
		}

		return false;
	}

	public int indexKeyHashMethod(String key) {
		int keyHash = key.hashCode();
		int keyHashPositive = Math.abs(keyHash);
		if (keyHashPositive < 0) {
			keyHashPositive = 0;
		}
		return keyHashPositive;
	}

	public long getBeginTimestamp() {
		return indexHeader.getBeginTimestamp();
	}

	public long getEndTimestamp() {
		return indexHeader.getEndTimestamp();
	}

	public long getEndPhyOffset() {
		return indexHeader.getEndPhyOffset();
	}

	public boolean isTimeMatched(long begin, long end) {
		boolean result = begin < indexHeader.getBeginTimestamp() && end > indexHeader.getEndTimestamp();
		result = result || begin > indexHeader.getBeginTimestamp() && begin <= indexHeader.getEndTimestamp();
		result = result || end >= indexHeader.getBeginTimestamp() && end <= indexHeader.getEndTimestamp();

		return result;
	}

	public void selectPhyOffset(List<Long> phyOffsets, String key, int maxNum, long begin, long end) {
		if (mappedFile.hold()) {
			int keyHash = indexKeyHashMethod(key);
			int slotPos = keyHash % hashSlotNum;
			int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

			try {
				int slotValue = mappedByteBuffer.getInt(absSlotPos);
				if (slotValue <= invalidIndex || slotValue > indexHeader.getIndexCount() || indexHeader.getIndexCount() <= 1) {

				}
				else {
					for (int nextIndexRead = slotValue; ; ) {
						if (phyOffsets.size() >= maxNum) {
							break;
						}

						int absIndexPos = IndexHeader.INDEX_HEADER_SIZE + hashSlotNum * hashSlotSize + nextIndexRead * indexSize;
						int keyHashRead = mappedByteBuffer.getInt(absIndexPos);
						long phyOffsetRead = mappedByteBuffer.getLong(absIndexPos + 4);
						long timeDiff = mappedByteBuffer.getInt(absIndexPos + 4 + 8);
						int prevIndexRead = mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

						if (timeDiff < 0) {
							break;
						}
						timeDiff *= 1000L;
						long timeRead = indexHeader.getBeginTimestamp() + timeDiff;
						boolean timeMatched = timeRead >= begin && timeRead <= end;

						if (keyHash == keyHashRead && timeMatched) {
							phyOffsets.add(phyOffsetRead);
						}

						if (prevIndexRead <= invalidIndex
						    || prevIndexRead > indexHeader.getIndexCount()
						    || prevIndexRead == nextIndexRead
						    || timeRead < begin) {
							break;
						}

						nextIndexRead = prevIndexRead;
					}
				}
			}
			catch (Exception e) {
				log.error("selectPhyOffset exception.", e);
			}
			finally {
				mappedFile.release();
			}
		}
	}
}
