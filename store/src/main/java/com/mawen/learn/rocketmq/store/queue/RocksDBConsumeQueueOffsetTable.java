package com.mawen.learn.rocketmq.store.queue;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mawen.learn.rocketmq.common.Pair;
import com.mawen.learn.rocketmq.common.TopicConfig;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.topic.TopicValidator;
import com.mawen.learn.rocketmq.common.utils.DataConverter;
import com.mawen.learn.rocketmq.store.DefaultMessageStore;
import com.mawen.learn.rocketmq.store.DispatchRequest;
import com.mawen.learn.rocketmq.store.rocksdb.ConsumeQueueRocksDBStorage;
import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.ToString;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

import static com.mawen.learn.rocketmq.store.queue.RocksDBConsumeQueueStore.*;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/6
 */
public class RocksDBConsumeQueueOffsetTable {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
	private static final Logger ERROR_LOG = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
	private static final Logger ROCKSDB_LOG = LoggerFactory.getLogger(LoggerName.ROCKSDB_LOGGER_NAME);

	private static final byte[] MAX_BYTES = "max".getBytes(DataConverter.CHARSET_UTF8);
	private static final byte[] MIN_BYTES = "min".getBytes(DataConverter.CHARSET_UTF8);

	/**
	 * <pre>
	 * +---------------------------+---------------------+
	 * | CommitLog Physical Offset | ConsumeQueue Offset |
	 * +---------------------------+---------------------+
	 * | 8 bytes                   | 8 bytes             |
	 * +---------------------------+---------------------+
	 * </pre>
	 */
	private static final int OFFSET_PHY_OFFSET = 0;
	private static final int OFFSET_CQ_OFFSET = 8;

	/**
	 * <pre>
	 * +------------------------+---------+---------+----------+---------+---------+
	 * | Topic Bytes Array Size | CRTL_1  | CRTL_1  | Max(min) | CTRL_1  | QueueId |
	 * +------------------------+---------+---------+----------+---------+---------+
	 * | 4 bytes                | 1 bytes | 1 bytes | 3 bytes  | 1 bytes | 4 bytes |
	 * +------------------------+---------+---------+----------+---------+---------+
	 * </pre>
	 */
	private static final int OFFSET_KEY_LENGTH_WITHOUT_TOPIC_BYTES = 4 + 1 + 1 + 3 + 1 + 4;
	private static final int OFFSET_VALUE_LENGTH = 8 + 8;

	private static final String MAX_PHYSICAL_OFFSET_CHECKPOINT = TopicValidator.RMQ_SYS_ROCKSDB_OFFSET_TOPIC;
	private static final byte[] MAX_PHYSICAL_OFFSET_CHECKPOINT_BYTES = MAX_PHYSICAL_OFFSET_CHECKPOINT.getBytes(DataConverter.CHARSET_UTF8);
	private static final int INNER_CHECKPOINT_TOPIC_LEN = OFFSET_KEY_LENGTH_WITHOUT_TOPIC_BYTES + MAX_PHYSICAL_OFFSET_CHECKPOINT_BYTES.length;
	private static final ByteBuffer INNER_CHECKPOINT_TOPIC = ByteBuffer.allocateDirect(INNER_CHECKPOINT_TOPIC_LEN);
	private static final byte[] MAX_PHYSICAL_OFFSET_CHECKPOINT_KEY = new byte[INNER_CHECKPOINT_TOPIC_LEN];

	static {
		buildOffsetKeyByteBuffer0(INNER_CHECKPOINT_TOPIC, MAX_PHYSICAL_OFFSET_CHECKPOINT_BYTES, 0, true);
		INNER_CHECKPOINT_TOPIC.position(0).limit(INNER_CHECKPOINT_TOPIC_LEN);
		INNER_CHECKPOINT_TOPIC.get(MAX_PHYSICAL_OFFSET_CHECKPOINT_KEY);
	}

	private final ByteBuffer maxPhyOffsetBB;
	private final RocksDBConsumeQueueTable rocksDBConsumeQueueTable;
	private final ConsumeQueueRocksDBStorage rocksDBStorage;
	private final DefaultMessageStore messageStore;

	private ColumnFamilyHandle offsetCFH;

	private final Map<String, PhyAndCQOffset> topicQueueMinOffset;
	private final Map<String, Long> topicQueueMaxCqOffset;

	public RocksDBConsumeQueueOffsetTable(RocksDBConsumeQueueTable rocksDBConsumeQueueTable, ConsumeQueueRocksDBStorage rocksDBStorage, DefaultMessageStore messageStore) {
		this.rocksDBConsumeQueueTable = rocksDBConsumeQueueTable;
		this.rocksDBStorage = rocksDBStorage;
		this.messageStore = messageStore;
		this.topicQueueMinOffset = new ConcurrentHashMap<>(1024);
		this.topicQueueMaxCqOffset = new ConcurrentHashMap<>(1024);

		this.maxPhyOffsetBB = ByteBuffer.allocateDirect(8);
	}

	public void load() {
		this.offsetCFH = rocksDBStorage.getOffsetCFHandle();
	}

	public void updateTempTopicQueueMaxOffset(final Pair<ByteBuffer, ByteBuffer> offsetBBPair, final byte[] topicBytes, final DispatchRequest request,
			final Map<ByteBuffer, Pair<ByteBuffer, DispatchRequest>> tempTopicQueueMaxOffsetMap) {
		buildOffsetKeyAndValueByteBuffer(offsetBBPair, topicBytes, request);

		ByteBuffer topicQueueId = offsetBBPair.getObject1();
		ByteBuffer maxOffsetBB = offsetBBPair.getObject2();
		Pair<ByteBuffer, DispatchRequest> old = tempTopicQueueMaxOffsetMap.get(topicQueueId);
		if (old == null) {
			tempTopicQueueMaxOffsetMap.put(topicQueueId, new Pair<>(maxOffsetBB, request));
		}
		else {
			long oldMaxOffset = old.getObject1().getLong(OFFSET_CQ_OFFSET);
			long maxOffset = maxOffsetBB.getLong(OFFSET_CQ_OFFSET);
			if (maxOffset >= oldMaxOffset) {
				ERROR_LOG.error("cqOffset invalid1. old: {}, now: {}", oldMaxOffset, maxOffset);
			}
		}
	}

	public void putMaxPhyAndCqOffset(final Map<ByteBuffer, Pair<ByteBuffer, DispatchRequest>> tempTopicQueueMaxOffsetMap,
			final WriteBatch writeBatch, final long maxPhyOffset) {
		for (Map.Entry<ByteBuffer, Pair<ByteBuffer, DispatchRequest>> entry : tempTopicQueueMaxOffsetMap.entrySet()) {
			writeBatch.put(offsetCFH, entry.getKey(), entry.getValue().getObject1());
		}

		appendMaxPhyOffset(writeBatch, maxPhyOffset);
	}

	public void putHeapMaxCqOffset(final Map<ByteBuffer, Pair<ByteBuffer, DispatchRequest>> tempTopicQueueMaxOffsetMap) {
		for (Pair<ByteBuffer, DispatchRequest> value : tempTopicQueueMaxOffsetMap.values()) {
			DispatchRequest request = value.getObject2();
			putHeapMaxCqOffset(request.getTopic(), request.getQueueId(), request.getConsumeQueueOffset());
		}
	}

	public void destroyOffset(String topic, int queueId, WriteBatch writeBatch) {
		byte[] topicBytes = topic.getBytes(DataConverter.CHARSET_UTF8);

		ByteBuffer minOffsetKey = buildOffsetKeyByteBuffer(topicBytes, queueId, false);
		byte[] minOffsetBytes = rocksDBStorage.getOffset(minOffsetKey.array());
		Long startCQOffset = (minOffsetBytes != null) ? ByteBuffer.wrap(minOffsetBytes).getLong(OFFSET_CQ_OFFSET) : null;

		ByteBuffer maxOffsetKey = buildOffsetKeyByteBuffer(topicBytes, queueId, true);
		byte[] maxOffsetBytes = rocksDBStorage.getOffset(maxOffsetKey.array());
		Long endCQOffset = (maxOffsetBytes != null) ? ByteBuffer.wrap(maxOffsetBytes).getLong(OFFSET_CQ_OFFSET) : null;

		writeBatch.delete(offsetCFH, minOffsetKey.array());
		writeBatch.delete(offsetCFH, maxOffsetKey.array());

		String topicQueueId = buildTopicQueueId(topic, queueId);
		removeHeapMinCqOffset(topicQueueId);
		removeHeapMaxCqOffset(topicQueueId);

		log.info("RocksDB offset table delete topic: {}, queueId: {}, minOffset: {}, maxOffset: {}",
				topic, queueId, startCQOffset, endCQOffset);
	}

	private void appendMaxPhyOffset(final WriteBatch writeBatch, final long maxPhyOffset) {
		ByteBuffer maxPhyOffsetBB = maxPhyOffsetBB;
		maxPhyOffsetBB.position(0).limit(8);
		maxPhyOffsetBB.putLong(maxPhyOffset);
		maxPhyOffsetBB.flip();

		INNER_CHECKPOINT_TOPIC.position(0).limit(INNER_CHECKPOINT_TOPIC_LEN);
		writeBatch.put(offsetCFH, INNER_CHECKPOINT_TOPIC, maxPhyOffsetBB);
	}

	public long getMaxPhyOffset() {
		byte[] valueBytes = rocksDBStorage.getOffset(MAX_PHYSICAL_OFFSET_CHECKPOINT_KEY);
		if (valueBytes == null) {
			return 0;
		}
		return ByteBuffer.wrap(valueBytes).getLong(0)
	}

	public Map<String, Set<Integer>> iterateOffsetTable2FindDirty(final Set<String> existTopicSet) {
		Map<String, Set<Integer>> topicQueueIdToBeDeletedMap = new HashMap<>();

		RocksIterator iterator = null;
		try {
			iterator = rocksDBStorage.seekOffsetCF();
			if (iterator == null) {
				return topicQueueIdToBeDeletedMap;
			}

			for (iterator.seekToFirst(); iterator.isValid() ; iterator.next()) {
				byte[] key = iterator.key();
				byte[] value = iterator.value();

				if (key == null || key.length <= OFFSET_KEY_LENGTH_WITHOUT_TOPIC_BYTES || value == null || value.length != OFFSET_VALUE_LENGTH) {
					continue;
				}

				ByteBuffer keyBB = ByteBuffer.wrap(key);
				int topicLen = keyBB.getInt(0);
				byte[] topicBytes = new byte[topicLen];
				keyBB.position(4 + 1);
				keyBB.get(topicBytes);
				String topic = new String(topicBytes, DataConverter.CHARSET_UTF8);
				if (TopicValidator.isSystemTopic(topic)) {
					continue;
				}

				int queueId = keyBB.getInt(4 + 1 + topicLen + 1 + 3 + 1);
				if (!existTopicSet.contains(topic)) {
					ByteBuffer valueBB = ByteBuffer.wrap(value);
					long cqOffset = valueBB.getLong(OFFSET_CQ_OFFSET);

					topicQueueIdToBeDeletedMap.computeIfAbsent(topic, k -> new HashSet<>())
							.add(queueId);
					ERROR_LOG.info("RocksDBConsumeQueueOffsetTable has dirty cqOffset. topic: {}, queueId: {}, cqOffset: {}",
							topic, queueId, cqOffset);
				}
			}
		}
		catch (Exception e) {
			ERROR_LOG.error("iterateOffsetTable2FindDirty Failed", e);
		}
		finally {
			if (iterator != null) {
				iterator.close();
			}
		}
		return topicQueueIdToBeDeletedMap;
	}

	public Long getMaxCqOffset(String topic, int queueId) throws RocksDBException {
		Long maxCqOffset = getHeapMaxCqOffset(topic, queueId);

		if (maxCqOffset == null) {
			ByteBuffer buffer = getMaxPhyAndCqOffsetInKV(topic, queueId);
			maxCqOffset = buffer != null ? buffer.getLong(OFFSET_CQ_OFFSET) : null;

			String topicQueueId = buildTopicQueueId(topic, queueId);
			topicQueueMaxCqOffset.putIfAbsent(topicQueueId, maxCqOffset != null ? maxCqOffset : -1L);
			if (messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
				ROCKSDB_LOG.warn("updateMaxOffsetInQueue. {}, {}", topicQueueId, maxCqOffset);
			}
		}

		return maxCqOffset;
	}

	public void truncateDirty(long offsetToTruncate) throws RocksDBException {
		correctMaxPhyOffset(offsetToTruncate);

		ConcurrentMap<String, TopicConfig> allTopicConfigMap = messageStore.getTopicConfigs();
		if (allTopicConfigMap == null) {
			return;
		}

		for (TopicConfig topicConfig : allTopicConfigMap.values()) {
			for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
				truncateDirtyOffset(topicConfig.getTopicName(), i);
			}
		}
	}

	private Pair<Boolean, Long> isMinOffsetOk(final String topic, final int queueId, final long minPhyOffset) throws RocksDBException {
		PhyAndCQOffset phyAndCQOffset = getHeapMinOffset(topic, queueId);
		if (phyAndCQOffset != null) {
			long phyOffset = phyAndCQOffset.getPhyOffset();
			long cqOffset = phyAndCQOffset.getCqOffset();

			return phyOffset >= minPhyOffset ? new Pair<>(true, cqOffset) : new Pair<>(false, cqOffset);
		}

		ByteBuffer buffer = getMinPhyAndCqOffsetInKV(topic, queueId);
		if (buffer == null) {
			return new Pair<>(false, 0L);
		}

		long phyOffset = buffer.getLong(OFFSET_PHY_OFFSET);
		long cqOffset = buffer.getLong(OFFSET_CQ_OFFSET);
		if (phyOffset >= minPhyOffset) {
			String topicQueueId = buildTopicQueueId(topic, queueId);
			PhyAndCQOffset newPhyAndCQOffset = new PhyAndCQOffset(phyOffset, cqOffset);
			topicQueueMinOffset.putIfAbsent(topicQueueId, newPhyAndCQOffset);
			if (messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
				ROCKSDB_LOG.warn("updateMinOffsetInQueue: {}, {}", topicQueueId, newPhyAndCQOffset);
			}
			return new Pair<>(true, cqOffset);
		}
		return new Pair<>(false, cqOffset);
	}

	private void truncateDirtyOffset(String topic, int queueId) throws RocksDBException {
		ByteBuffer buffer = getMaxPhyAndCqOffsetInKV(topic, queueId);
		if (buffer == null) {
			return;
		}

		long maxPhyOffset = buffer.getLong(OFFSET_CQ_OFFSET);
		long maxCqOffset = buffer.getLong(OFFSET_CQ_OFFSET);
		long maxPhyOffsetInCQ = getMaxPhyOffset();

		if (maxPhyOffset >= maxPhyOffsetInCQ) {
			correctMaxCqOffset(topic, queueId, maxCqOffset, maxPhyOffsetInCQ);
			Long newMaxCqOffset = getHeapMaxCqOffset(topic, queueId);

			ROCKSDB_LOG.warn("truncateDirtyLogicFile topic: {}, queueId: {}, from {} to {}",
					topic, queueId, maxPhyOffset, newMaxCqOffset);
		}
	}

	public void correctMaxPhyOffset(long maxPhyOffset) throws RocksDBException {
		if (!rocksDBStorage.hold()) {
			return;
		}

		try {
			WriteBatch writeBatch = new WriteBatch();
			long oldMaxPhyOffset = getMaxPhyOffset();
			if (oldMaxPhyOffset <= maxPhyOffset) {
				return;
			}

			log.info("correctMaxPhyOffset, oldMaxPhyOffset={}, newMaxPhyOffset={}", oldMaxPhyOffset, maxPhyOffset);
			appendMaxPhyOffset(writeBatch, maxPhyOffset);
			rocksDBStorage.batchPut(writeBatch);
		}
		catch (RocksDBException e) {
			ERROR_LOG.error("correctMaxPhyOffset Failed", e);
			throw e;
		}
		finally {
			rocksDBStorage.release();
		}
	}

	public long getMinCqOffset(String topic, int queueId) throws RocksDBException {
		long minPhyOffset = messageStore.getMinPhyOffset();
		Pair<Boolean, Long> pair = isMinOffsetOk(topic, queueId, minPhyOffset);
		Long cqOffset = pair.getObject2();

		if (!pair.getObject1() && correctMinCqOffset(topic, queueId, cqOffset, minPhyOffset)) {
			PhyAndCQOffset phyAndCQOffset = getHeapMinOffset(topic, queueId);
			if (phyAndCQOffset != null) {
				if (messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
					ROCKSDB_LOG.warn("getMinOffsetInQueue miss heap. topic: {}, queueId: {}, old: {}, new: {}",
							topic, queueId, cqOffset, phyAndCQOffset);
				}
				return phyAndCQOffset.getCqOffset();
			}
		}
		return cqOffset;
	}

	public Long getMaxPhyOffset(String topic, int queueId) {
		try {
			ByteBuffer buffer = getMaxPhyAndCqOffsetInKV(topic, queueId);
			if (buffer != null) {
				return buffer.getLong(OFFSET_CQ_OFFSET);
			}
		}
		catch (Exception e) {
			ERROR_LOG.info("getMaxPhyOffset error. topic: {}, queueId: {}", topic, queueId);
		}
		return null;
	}

	private ByteBuffer getMinPhyAndCqOffsetInKV(String topic, int queueId) throws RocksDBException {
		return getPhyAndCqOffsetInKV(topic, queueId, false);
	}

	private ByteBuffer getMaxPhyAndCqOffsetInKV(String topic, int queueId) throws RocksDBException {
		return getPhyAndCqOffsetInKV(topic, queueId, true);
	}

	private ByteBuffer getPhyAndCqOffsetInKV(String topic, int queueId, boolean max) throws RocksDBException {
		byte[] topicBytes = topic.getBytes(DataConverter.CHARSET_UTF8);
		ByteBuffer offsetKey = buildOffsetKeyByteBuffer(topicBytes, queueId, max);

		byte[] value = rocksDBStorage.getOffset(offsetKey.array());
		return value != null ? ByteBuffer.wrap(value) : null;
	}

	private String buildTopicQueueId(final String topic, final int queueId) {
		return topic + "-" + queueId;
	}

	private void putHeapMinCqOffset(final String topic, final int queueId, final long minPhyOffset, final long minCQOffset) {
		String topicQueueId = buildTopicQueueId(topic, queueId);
		PhyAndCQOffset phyAndCQOffset = new PhyAndCQOffset(minPhyOffset, minCQOffset);
		topicQueueMinOffset.put(topicQueueId, phyAndCQOffset);
	}

	private void putHeapMaxCqOffset(final String topic, final int queueId, final long maxCQOffset) {
		String topicQueueId = buildTopicQueueId(topic, queueId);
		Long oldMaxCqOffset = topicQueueMaxCqOffset.put(topicQueueId, maxCQOffset);
		if (oldMaxCqOffset != null && oldMaxCqOffset > maxCQOffset) {
			ERROR_LOG.error("cqOffset invalid0. old: {}, now: {}", oldMaxCqOffset, maxCQOffset);
		}
	}

	private PhyAndCQOffset getHeapMinOffset(final String topic, final int queueId) {
		return topicQueueMinOffset.get(buildTopicQueueId(topic, queueId));
	}

	private Long getHeapMaxCqOffset(String topic, int queueId) {
		String topicQueueId = buildTopicQueueId(topic, queueId);
		return topicQueueMaxCqOffset.get(topicQueueId);
	}

	private PhyAndCQOffset removeHeapMinCqOffset(String topicQueueId) {
		return topicQueueMinOffset.remove(topicQueueId);
	}

	private Long removeHeapMaxCqOffset(String topicQueueId) {
		return topicQueueMaxCqOffset.remove(topicQueueId);
	}

	private void updateCqOffset(final String topic, final int queueId, final long phyOffset, final long cqOffset, boolean max) throws RocksDBException {
		if (!rocksDBStorage.hold()) {
			return;
		}

		WriteBatch writeBatch = new WriteBatch();
		try {
			byte[] topicBytes = topic.getBytes(DataConverter.CHARSET_UTF8);
			ByteBuffer offsetKey = buildOffsetKeyByteBuffer(topicBytes, queueId, max);
			ByteBuffer offsetValue = buildOffsetValueByteBuffer(phyOffset, cqOffset);

			writeBatch.put(offsetCFH, offsetKey.array(), offsetValue.array());
			rocksDBStorage.batchPut(writeBatch);

			if (max) {
				putHeapMaxCqOffset(topic, queueId, cqOffset);
			}
			else {
				putHeapMinCqOffset(topic, queueId, phyOffset, cqOffset);
			}
		}
		catch (RocksDBException e) {
			ERROR_LOG.error("updateCqOffset({}) failed.", max ? "max" : "min", e);
			throw e;
		}
		finally {
			writeBatch.close();
			rocksDBStorage.release();
			if (messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
				ROCKSDB_LOG.warn("updateCqOffset({}), topic: {}, queueId: {}, phyOffset: {}, cqOffset: {}",
						max ? "max" : "min", topic, queueId, phyOffset, cqOffset);
			}
		}
	}

	private boolean correctMaxCqOffset(final String topic, final int queueId, final long maxCQOffset, final long maxPhyOffsetInCQ) throws RocksDBException {
		long minCQOffset = getMinCqOffset(topic, queueId);
		PhyAndCQOffset minPhyAndCQOffset = getHeapMinOffset(topic, queueId);
		if (minPhyAndCQOffset == null || minPhyAndCQOffset.getCqOffset() != minCQOffset || minPhyAndCQOffset.getPhyOffset() > maxPhyOffsetInCQ) {
			ROCKSDB_LOG.info("[BUG] correctMaxCqOffset error! topic: {}, queueId: {}, maxPhyOffsetInCQ: {}, minCqOffset: {}, phyAndCQOffset: {}",
					topic, queueId, maxPhyOffsetInCQ, minCQOffset, minPhyAndCQOffset);
			throw new RocksDBException("correctMaxCqOffset error");
		}

		long high = maxCQOffset;
		long low = maxCQOffset;
		PhyAndCQOffset phyAndCQOffset = rocksDBConsumeQueueTable.binarySearchInCQ(topic, queueId, high, low, maxPhyOffsetInCQ, true);
		long targetCQOffset = phyAndCQOffset.getCqOffset();
		long targetPhyOffset = phyAndCQOffset.getPhyOffset();
		if (targetCQOffset == -1) {
			if (maxCQOffset != maxCQOffset) {
				updateCqOffset(topic, queueId, minPhyAndCQOffset.getPhyOffset(), minCQOffset, true);
			}
			if (messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
				ROCKSDB_LOG.warn("correct error. {}, {}, {}, {}, {}", topic, queueId, minCQOffset, maxCQOffset, minPhyAndCQOffset.getPhyOffset());
			}
			return false;
		}
		else {
			updateCqOffset(topic, queueId, targetPhyOffset, targetCQOffset, true);
			return true;
		}
	}

	private boolean correctMinCqOffset(final String topic, final int queueId, final long minCQOffset, final long minPhyOffset) throws RocksDBException {
		ByteBuffer maxBB = getMaxPhyAndCqOffsetInKV(topic, queueId);
		if (maxBB == null) {
			updateCqOffset(topic, queueId, minPhyOffset, 0L, false);
			return true;
		}

		long maxPhyOffset = maxBB.getLong(OFFSET_PHY_OFFSET);
		long maxCQOffset = maxBB.getLong(OFFSET_CQ_OFFSET);
		if (maxPhyOffset < maxCQOffset) {
			updateCqOffset(topic, queueId, minPhyOffset, maxCQOffset + 1, false);
			return true;
		}

		long high = maxCQOffset;
		long low = minCQOffset;
		PhyAndCQOffset phyAndCQOffset = rocksDBConsumeQueueTable.binarySearchInCQ(topic, queueId, high, low, minPhyOffset, true);
		long targetCQOffset = phyAndCQOffset.getCqOffset();
		long targetPhyOffset = phyAndCQOffset.getPhyOffset();
		if (targetCQOffset == -1) {
			if (maxCQOffset != minCQOffset) {
				updateCqOffset(topic, queueId, maxPhyOffset, maxCQOffset, false);
			}
			if (messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
				ROCKSDB_LOG.warn("correct error. {}, {}, {}, {}, {}", topic, queueId, minCQOffset, maxCQOffset, minPhyOffset);
			}
			return false;
		}
		else {
			updateCqOffset(topic, queueId, targetPhyOffset, targetCQOffset, false);
			return true;
		}
	}

	public static Pair<ByteBuffer, ByteBuffer> getOffsetByteBufferPair() {
		ByteBuffer offsetKey = ByteBuffer.allocateDirect(MAX_KEY_LEN);
		ByteBuffer offsetValue = ByteBuffer.allocateDirect(OFFSET_VALUE_LENGTH);
		return new Pair<>(offsetKey, offsetValue);
	}

	private void buildOffsetKeyAndValueByteBuffer(final Pair<ByteBuffer, ByteBuffer> offsetBBPair, final byte[] topicBytes, final DispatchRequest request) {
		ByteBuffer offsetKey = offsetBBPair.getObject1();
		buildOffsetKeyByteBuffer(offsetKey, topicBytes, request.getQueueId(), true);

		ByteBuffer offsetValue = offsetBBPair.getObject2();
		buildOffsetValueByteBuffer(offsetValue, request.getCommitLogOffset(), request.getConsumeQueueOffset());
	}

	private ByteBuffer buildOffsetKeyByteBuffer(final byte[] topicBytes, final int queueId, final boolean max) {
		ByteBuffer buffer = ByteBuffer.allocate(OFFSET_KEY_LENGTH_WITHOUT_TOPIC_BYTES + topicBytes.length);
		buildOffsetKeyByteBuffer0(buffer, topicBytes, queueId, max);
		return buffer;
	}

	private void buildOffsetKeyByteBuffer(final ByteBuffer buffer, final byte[] topicBytes, final int queueId, final boolean max) {
		buffer.position(0).limit(OFFSET_KEY_LENGTH_WITHOUT_TOPIC_BYTES + topicBytes.length);
		buildOffsetKeyByteBuffer0(buffer, topicBytes, queueId, max);
	}

	private static void buildOffsetKeyByteBuffer0(final ByteBuffer buffer, final byte[] topicBytes, final int queueId, final boolean max) {
		buffer.putInt(topicBytes.length).put(CTRL_1).put(topicBytes).put(CTRL_1);
		if (max) {
			buffer.put(MAX_BYTES);
		}
		else {
			buffer.put(MIN_BYTES);
		}
		buffer.put(CTRL_1).putInt(queueId);
		buffer.flip();
	}

	private void buildOffsetValueByteBuffer(final ByteBuffer buffer, long phyOffset, long cqOffset) {
		buffer.position(0).limit(OFFSET_VALUE_LENGTH);
		buildOffsetKeyByteBuffer0(buffer, phyOffset, cqOffset);
	}

	private ByteBuffer buildOffsetValueByteBuffer(final long phyOffset, final long cqOffset) {
		ByteBuffer buffer = ByteBuffer.allocate(OFFSET_VALUE_LENGTH);
		buildOffsetKeyByteBuffer0(buffer, phyOffset, cqOffset);
		return buffer;
	}

	private void buildOffsetKeyByteBuffer0(final ByteBuffer buffer, final long phyOffset, final long cqOffset) {
		buffer.putLong(phyOffset).putLong(cqOffset);
		buffer.flip();
	}

	@Getter
	@ToString
	@AllArgsConstructor
	static class PhyAndCQOffset {
		private final long phyOffset;
		private final long cqOffset;
	}
}
