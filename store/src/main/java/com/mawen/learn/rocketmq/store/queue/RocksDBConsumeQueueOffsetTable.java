package com.mawen.learn.rocketmq.store.queue;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.mawen.learn.rocketmq.common.Pair;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.topic.TopicValidator;
import com.mawen.learn.rocketmq.common.utils.DataConverter;
import com.mawen.learn.rocketmq.store.DefaultMessageStore;
import com.mawen.learn.rocketmq.store.DispatchRequest;
import com.mawen.learn.rocketmq.store.rocksdb.ConsumeQueueRocksDBStorage;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.ColumnFamilyHandle;

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

	}

	private static void buildOffsetKeyByteBuffer0() {

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
