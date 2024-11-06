package com.mawen.learn.rocketmq.store.queue;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import com.mawen.learn.rocketmq.common.Pair;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.store.DefaultMessageStore;
import com.mawen.learn.rocketmq.store.DispatchRequest;
import com.mawen.learn.rocketmq.store.config.StorePathConfigHelper;
import com.mawen.learn.rocketmq.store.rocksdb.ConsumeQueueRocksDBStorage;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.WriteBatch;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/6
 */
public class RocksDBConsumeQueueStore extends AbstractConsumeQueueStore {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKSDB_LOGGER_NAME);
	private static final Logger ERROR_LOG = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

	public static final byte CTRL_0 = '\u0000';
	public static final byte CTRL_1 = '\u0001';
	public static final byte CTRL_2 = '\u0002';
	public static final int MAX_KEY_LEN = 300;
	private static final int BATCH_SIZE = 16;

	private final ScheduledExecutorService scheduledExecutorService;
	private final String storePath;
	private final ConsumeQueueRocksDBStorage rocksDBStorage;
	private final RocksDBConsumeQueueStore rocksDBConsumeQueueTable;
	private final RocksDBConsumeQueueOffsetTable rocksDBCOnsumeQueueOffsetTable;

	private final WriteBatch writeBatch;
	private final List<DispatchRequest> bufferDRList;
	private final List<Pair<ByteBuffer, ByteBuffer>> cqBBPairList;
	private final List<Pair<ByteBuffer, ByteBuffer>> offsetBBPairList;
	private final Map<ByteBuffer, Pair<ByteBuffer, DispatchRequest>> tmpTopicQueueMaxOffsetMap;
	private volatile boolean isCQError = false;

	public RocksDBConsumeQueueStore(DefaultMessageStore messageStore) {
		super(messageStore);

		this.storePath = StorePathConfigHelper.getStorePathBatchConsumeQueue(messageStoreConfig.getStorePathRootDir());
		this.rocksDBStorage = new ConsumeQueueRocksDBStorage(messageStore, storePath, 4);
		this.rocksDBConsumeQueue
	}
}
