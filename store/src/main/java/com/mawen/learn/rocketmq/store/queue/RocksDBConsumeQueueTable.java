package com.mawen.learn.rocketmq.store.queue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.mawen.learn.rocketmq.common.BoundaryType;
import com.mawen.learn.rocketmq.common.Pair;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.utils.DataConverter;
import com.mawen.learn.rocketmq.store.DefaultMessageStore;
import com.mawen.learn.rocketmq.store.DispatchRequest;
import com.mawen.learn.rocketmq.store.rocksdb.ConsumeQueueRocksDBStorage;
import lombok.RequiredArgsConstructor;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

import static com.mawen.learn.rocketmq.store.queue.RocksDBConsumeQueueStore.*;

/**
 * <pre>
 * <p>Key Unit：
 * ┌─────────────────────────┬────────┬───────────────────┬─────────┬─────────┬────────┬─────────────────────┐
 * │ Topic Bytes Array Size  │ CTRL_1 │ Topic Bytes Array │ CTRL_1  │ QueueId │ CTRL_1 │ ConsumeQueue Offset │
 * ├─────────────────────────┼────────┼───────────────────┼─────────┼─────────┼────────┼─────────────────────┤
 * │ 4 bytes                 │ 1 byte │ n bytes           │ 1 bytes │ 4 bytes │ 1 byte │ 8 bytes             │
 * └─────────────────────────┴────────┴───────────────────┴─────────┴─────────┴────────┴─────────────────────┘
 *
 * <p>Value Unit：
 * ┌───────────────────────────┬───────────┬──────────────┬────────────────┐
 * │ CommitLog Physical Offset │ Body Size │ Tag HashCode │ Msg Store Time │
 * ├───────────────────────────┼───────────┼──────────────┼────────────────┤
 * │ 8 bytes                   │ 4 bytes   │ 8 bytes      │ 8 bytes        │
 * └───────────────────────────┴───────────┴──────────────┴────────────────┘
 * </pre>
 *
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/6
 */
@RequiredArgsConstructor
public class RocksDBConsumeQueueTable {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
	private static final Logger ROCKSDB_LOG = LoggerFactory.getLogger(LoggerName.ROCKSDB_LOGGER_NAME);
	private static final Logger ERROR_LOG = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

	public static final int CQ_UNIT_SIZE = 8 + 4 + 8 + 8;

	private static final int PHY_OFFSET_OFFSET = 0;
	private static final int PHY_MSG_LEN_OFFSET = 8;
	private static final int MSG_TAG_HASHCODE_OFFSET = 12;
	private static final int MSG_STORE_TIME_SIZE_OFFSET = 20;

	/**
	 * <pre>
	 * +------------------------+--------+---------+---------+--------+---------------------+
	 * | Topic Bytes Array Size | CTRL_1 | CTRL_1  | QueueId | CTRL_1 | ConsumeQueue Offset |
	 * +------------------------+--------+---------+---------+--------+---------------------+
	 * | 4 bytes                | 1 byte | 1 bytes | 4 bytes | 1 byte | 8 bytes             |
	 * +------------------------+--------+---------+---------+--------+---------------------+
	 * </pre>
	 */
	private static final int CQ_KEY_LENGTH_WITHOUT_TOPIC_BYTES = 4 + 1 + 1 + 4 + 1 + 8;

	/**
	 * <pre>
	 * ┌─────────────────────────┬────────┬────────┬─────────┬────────────────┐
	 * │ Topic Bytes Array Size  │ CTRL_1 │ CTRL_1 │ QueueId │ CTRL_0(CTRL_2) │
	 * ├─────────────────────────┼────────┼────────┼─────────┼────────────────┤
	 * │ 4 bytes                 │ 1 byte │ 1 byte │ 4 bytes │ 1 byte         │
	 * └─────────────────────────┴────────┴────────┴─────────┴────────────────┘
	 * </pre>
	 */
	private static final int DELETE_CQ_KEY_LENGTH_WITHOUT_TOPIC_BYTES = 4 + 1 + 1 + 4 + 1;

	private final ConsumeQueueRocksDBStorage rocksDBStorage;
	private final DefaultMessageStore messageStore;

	private ColumnFamilyHandle defaultCFH;

	public void load() {
		this.defaultCFH = rocksDBStorage.getDefaultCFHandle();
	}

	public void buildAndPutCQByteBuffer(final Pair<ByteBuffer, ByteBuffer> cqBBPair, final byte[] topicBytes, final DispatchRequest request, final WriteBatch writeBatch) throws RocksDBException {
		ByteBuffer cqKey = cqBBPair.getObject1();
		buildCQKeyByteBuffer(cqKey, topicBytes, request.getQueueId(), request.getConsumeQueueOffset());

		ByteBuffer cqValue = cqBBPair.getObject2();
		buildCQValueByteBuffer(cqValue, request.getCommitLogOffset(), request.getMsgSize(), request.getTagsCode(), request.getStoreTimestamp());

		writeBatch.put(defaultCFH, cqKey, cqValue);
	}

	public ByteBuffer getCQInKV(final String topic, final int queueId, final long cqOffset) throws RocksDBException {
		byte[] topicBytes = topic.getBytes(DataConverter.CHARSET_UTF8);
		ByteBuffer keyBB = buildCQKeyByteBuffer(topicBytes, queueId, cqOffset);
		byte[] value = rocksDBStorage.getCQ(keyBB.array());
		return value != null ? ByteBuffer.wrap(value) : null;
	}

	public List<ByteBuffer> rangeQuery(final String topic, final int queueId, final long startIndex, final int num) throws RocksDBException {
		byte[] topicBytes = topic.getBytes(DataConverter.CHARSET_UTF8);
		List<ColumnFamilyHandle> defaultCFHList = new ArrayList<>(num);
		ByteBuffer[] resultList = new ByteBuffer[num];
		List<Integer> kvIndexList = new ArrayList<>(num);
		List<byte[]> kvKeyList = new ArrayList<>(num);

		for (int i = 0; i < num; i++) {
			ByteBuffer keyBB = buildCQKeyByteBuffer(topicBytes, queueId, startIndex + i);
			kvIndexList.add(i);
			kvKeyList.add(keyBB.array());
			defaultCFHList.add(defaultCFH);
		}

		int keyNum = kvIndexList.size();
		if (keyNum > 0) {
			List<byte[]> kvValueList = rocksDBStorage.multiGet(defaultCFHList, kvKeyList);
			int valueNum = kvValueList.size();
			if (keyNum != valueNum) {
				throw new RocksDBException("rocksdb bug, multiGet");
			}

			for (int i = 0; i < valueNum; i++) {
				byte[] value = kvValueList.get(i);
				if (value == null) {
					continue;
				}
				ByteBuffer buffer = ByteBuffer.wrap(value);
				resultList[kvIndexList.get(i)] = buffer;
			}
		}

		return Arrays.stream(resultList).filter(Objects::nonNull).collect(Collectors.toList());
	}

	public void destroyCQ(final String topic, final int queueId, WriteBatch writeBatch) throws RocksDBException {
		byte[] topicBytes = topic.getBytes(DataConverter.CHARSET_UTF8);
		ByteBuffer cqStartKey = buildDeleteCQKey(true, topicBytes, queueId);
		ByteBuffer cqEndKey = buildDeleteCQKey(false, topicBytes, queueId);

		writeBatch.deleteRange(defaultCFH, cqStartKey.array(), cqEndKey.array());

		log.info("Rocksdb consumeQueue table delete topic. {}, {}", topic, queueId);
	}

	public long binarySearchInCQByTime(String topic, int queueId, long high, long low, long timestamp,
			long minPhysicOffset, BoundaryType boundaryType) throws RocksDBException {
		long result = -1L, targetOffset = -1L, leftOffset = -1L, rightOffset = -1L;
		long ceiling = high, floor = low;

		while (high >= low) {
			long midOffset = low + (high - low) >>> 1;
			ByteBuffer buffer = getCQInKV(topic, queueId, midOffset);

			if (buffer == null) {
				ERROR_LOG.warn("binarySearchInCQByTimestamp Failed. topic: {}, queueId: {}, timestamp: {}, result: null",
						topic, queueId, timestamp);
				low = midOffset + 1;
				continue;
			}

			long phyOffset = buffer.getLong(PHY_OFFSET_OFFSET);
			if (phyOffset < minPhysicOffset) {
				low = midOffset + 1;
				leftOffset = midOffset;
				continue;
			}

			long storeTime = buffer.getLong(MSG_STORE_TIME_SIZE_OFFSET);
			if (storeTime < 0) {
				return 0;
			}
			else if (storeTime == timestamp) {
				targetOffset = midOffset;
				break;
			}
			else if (storeTime > timestamp) {
				high = midOffset - 1;
				rightOffset = midOffset;
			}
			else {
				low = midOffset + 1;
				leftOffset = midOffset;
			}
		}

		if (targetOffset != -1) {
			switch (boundaryType) {
				case LOWER:
					while (true) {
						long nextOffset = targetOffset - 1;
						if (nextOffset < floor) {
							break;
						}
						ByteBuffer buffer = getCQInKV(topic, queueId, nextOffset);
						long storeTime = buffer.getLong(MSG_STORE_TIME_SIZE_OFFSET);
						if (storeTime != timestamp) {
							break;
						}
						targetOffset = nextOffset;
					}
					break;
				case UPPER:
					while (true) {
						long nextOffset = targetOffset + 1;
						if (nextOffset > ceiling) {
							break;
						}
						ByteBuffer buffer = getCQInKV(topic, queueId, nextOffset);
						long storeTime = buffer.getLong(MSG_STORE_TIME_SIZE_OFFSET);
						if (storeTime != timestamp) {
							break;
						}
						targetOffset = nextOffset;
					}
					break;
				default:
					log.warn("Unknown boundary type");
					break;
			}
			result = targetOffset;
		}
		else {
			switch (boundaryType) {
				case LOWER:
					result = rightOffset;
					break;
				case UPPER:
					result = leftOffset;
					break;
				default:
					log.warn("Unknown boundary type");
					break;
			}
		}
		return result;

	}

	public RocksDBConsumeQueueOffsetTable.PhyAndCQOffset binarySearchInCQ(String topic, int queueId, long high, long low, long targetPhyOffset, boolean min) throws RocksDBException {
		long resultPhyOffset = -1L, resultCQOffset = -1L;

		while (high >= low) {
			long midCQOffset = low + (high - low) >>> 1;
			ByteBuffer buffer = getCQInKV(topic, queueId, midCQOffset);
			if (messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
				ROCKSDB_LOG.warn("binarySearchInCQ. {}, {}, {}, {}, {}", topic, queueId, midCQOffset, low, high);
			}

			if (buffer == null) {
				low = midCQOffset + 1;
				continue;
			}

			long phyOffset = buffer.getLong(PHY_OFFSET_OFFSET);
			if (phyOffset == targetPhyOffset) {
				if (min) {
					resultCQOffset = midCQOffset;
					resultPhyOffset = phyOffset;
				}
				break;
			}
			else if (phyOffset > targetPhyOffset) {
				high = midCQOffset - 1;
				if (min) {
					resultCQOffset = midCQOffset;
					resultPhyOffset = phyOffset;
				}
			}
			else {
				low = midCQOffset + 1;
				if (!min) {
					resultCQOffset = midCQOffset;
					resultPhyOffset = phyOffset;
				}
			}
		}
		return new RocksDBConsumeQueueOffsetTable.PhyAndCQOffset(resultPhyOffset, resultCQOffset);
	}

	public static Pair<ByteBuffer, ByteBuffer> getCQByteBufferPair() {
		ByteBuffer cqKey = ByteBuffer.allocateDirect(RocksDBConsumeQueueStore.MAX_KEY_LEN);
		ByteBuffer cqValue = ByteBuffer.allocateDirect(CQ_UNIT_SIZE);
		return new Pair<>(cqKey, cqValue);
	}

	private ByteBuffer buildCQKeyByteBuffer(final byte[] topicBytes, final int queueId, final long cqOffset) {
		ByteBuffer buffer = ByteBuffer.allocate(CQ_KEY_LENGTH_WITHOUT_TOPIC_BYTES + topicBytes.length);
		buildCQKeyByteBuffer0(buffer, topicBytes, queueId, cqOffset);
		return buffer;
	}

	private void buildCQKeyByteBuffer(final ByteBuffer buffer, final byte[] topicBytes, final int queueId, final long cqOffset) {
		buffer.position(0).limit(CQ_KEY_LENGTH_WITHOUT_TOPIC_BYTES + topicBytes.length);
		buildCQKeyByteBuffer0(buffer, topicBytes, queueId, cqOffset);
	}

	private void buildCQKeyByteBuffer0(final ByteBuffer buffer, final byte[] topicBytes, final int queueId, final long cqOffset) {
		buffer.putInt(topicBytes.length).put(CTRL_1).put(topicBytes).put(CTRL_1).putInt(queueId).put(CTRL_1).putLong(cqOffset);
		buffer.flip();
	}

	private void buildCQValueByteBuffer(final ByteBuffer buffer, final long phyOffset, final int msgSize, final long tagsCode, final long storeTimestamp) {
		buffer.position(0).limit(CQ_UNIT_SIZE);
		buildCQValueByteBuffer0(buffer, phyOffset, msgSize, tagsCode, storeTimestamp);
	}

	private void buildCQValueByteBuffer0(final ByteBuffer buffer, final long phyOffset, final int msgSize, final long tagsCode, final long storeTimestamp) {
		buffer.putLong(phyOffset).putInt(msgSize).putLong(tagsCode).putLong(storeTimestamp);
		buffer.flip();
	}

	private ByteBuffer buildDeleteCQKey(final boolean start, final byte[] topicBytes, final int queueId) {
		ByteBuffer buffer = ByteBuffer.allocate(DELETE_CQ_KEY_LENGTH_WITHOUT_TOPIC_BYTES + topicBytes.length);

		buffer.putInt(topicBytes.length).put(CTRL_1).put(topicBytes).put(CTRL_1).putInt(queueId).put(start ? CTRL_0 : CTRL_2);
		buffer.flip();
		return buffer;
	}
}
