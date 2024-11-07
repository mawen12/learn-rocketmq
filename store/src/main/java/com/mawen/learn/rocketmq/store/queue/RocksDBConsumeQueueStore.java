package com.mawen.learn.rocketmq.store.queue;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.mawen.learn.rocketmq.common.BoundaryType;
import com.mawen.learn.rocketmq.common.Pair;
import com.mawen.learn.rocketmq.common.ThreadFactoryImpl;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.utils.DataConverter;
import com.mawen.learn.rocketmq.store.DefaultMessageStore;
import com.mawen.learn.rocketmq.store.DispatchRequest;
import com.mawen.learn.rocketmq.store.config.BrokerRole;
import com.mawen.learn.rocketmq.store.config.StorePathConfigHelper;
import com.mawen.learn.rocketmq.store.rocksdb.ConsumeQueueRocksDBStorage;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.WriteBatch;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/6
 */
public class RocksDBConsumeQueueStore extends AbstractConsumeQueueStore {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKSDB_LOGGER_NAME);
	private static final Logger ERROR_LOG = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
	private static final Logger ROCKSDB_LOG = LoggerFactory.getLogger(LoggerName.ROCKSDB_LOGGER_NAME);

	public static final byte CTRL_0 = '\u0000';
	public static final byte CTRL_1 = '\u0001';
	public static final byte CTRL_2 = '\u0002';
	public static final int MAX_KEY_LEN = 300;
	private static final int BATCH_SIZE = 16;

	private final ScheduledExecutorService scheduledExecutorService;
	private final String storePath;
	private final ConsumeQueueRocksDBStorage rocksDBStorage;
	private final RocksDBConsumeQueueTable rocksDBConsumeQueueTable;
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
		this.rocksDBConsumeQueueTable = new RocksDBConsumeQueueTable(rocksDBStorage, messageStore);
		this.rocksDBCOnsumeQueueOffsetTable = new RocksDBConsumeQueueOffsetTable(rocksDBConsumeQueueTable, rocksDBStorage, messageStore);

		this.writeBatch = new WriteBatch();
		this.bufferDRList = new ArrayList<>(BATCH_SIZE);
		this.cqBBPairList = new ArrayList<>(BATCH_SIZE);
		this.offsetBBPairList = new ArrayList<>(BATCH_SIZE);

		for (int i = 0; i < BATCH_SIZE; i++) {
			cqBBPairList.add(RocksDBConsumeQueueTable.getCQByteBufferPair());
			offsetBBPairList.add(RocksDBConsumeQueueOffsetTable.getOffsetByteBufferPair());
		}

		this.tmpTopicQueueMaxOffsetMap = new HashMap<>();
		this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
				new ThreadFactoryImpl("RocksDBConsumeQueueStoreScheduledThread", messageStore.getBrokerIdentify())
		);
	}

	@Override
	public void start() {
		log.info("RocksDB ConsumeQueueStore start!");
		scheduledExecutorService.scheduleAtFixedRate(() -> {
			rocksDBStorage.statRocksdb(ROCKSDB_LOG);
		}, 10, messageStoreConfig.getStatRocksDBCQIntervalSec(), TimeUnit.SECONDS);

		scheduledExecutorService.scheduleWithFixedDelay(() -> {
			cleanDirty(messageStore.getTopicConfigs().keySet());
		}, 10, messageStoreConfig.getCleanRocksDBDirtyCQIntervalMin(), TimeUnit.MINUTES);
	}

	private void cleanDirty(final Set<String> existTopicSet) {
		try {
			Map<String, Set<Integer>> topicQueueIdToBeDeleteMap = rocksDBCOnsumeQueueOffsetTable.iterateOffsetTable2FindDirty(existTopicSet);

			for (Map.Entry<String, Set<Integer>> entry : topicQueueIdToBeDeleteMap.entrySet()) {
				String topic = entry.getKey();
				for (Integer queueId : entry.getValue()) {
					destroy(new RocksDBConsumeQueue(topic, queueId));
				}
			}
		}
		catch (Exception e) {
			log.error("cleanUnusedTopic Failed.", e);
		}
	}

	@Override
	public boolean load() {
		boolean result = rocksDBStorage.start();
		rocksDBConsumeQueueTable.load();
		rocksDBCOnsumeQueueOffsetTable.load();
		log.info("load rocksdb consume queue: {}.", result ? "OK" : "Failed");
		return result;
	}

	@Override
	public boolean loadAfterDestroy() {
		return load();
	}

	@Override
	public void recover() {
		// NOP
	}

	@Override
	public boolean recoverConcurrently() {
		return true;
	}

	@Override
	public boolean shutdown() {
		scheduledExecutorService.shutdown();
		return shutdownInner;
	}

	private boolean shutdownInner() {
		return rocksDBStorage.shutdown();
	}

	@Override
	public void putMessagePositionInfoWrapper(ConsumeQueueInterface consumeQueue, DispatchRequest request) {
		if (request == null || bufferDRList.size() >= BATCH_SIZE) {
			putMessagePosition();
		}
		if (request != null) {
			bufferDRList.add(request);
		}
	}

	public void putMessagePosition() throws RocksDBException {
		int maxRetries = 30;
		for (int i = 0; i < maxRetries; i++) {
			if (putMessagePosition0()) {
				if (isCQError) {
					messageStore.getRunningFlags().clearLogicsQueueError();
					isCQError = false;
				}
				return;
			}
			else {
				ERROR_LOG.warn("{} put cq failed, retryTime: {}", i);
				try {
					Thread.sleep(100);
				}
				catch (InterruptedException ignored) {}
			}
		}

		if (!isCQError) {
			ERROR_LOG.error("[BUG] put CQ failed.");
			messageStore.getRunningFlags().makeLogicsQueueError();
			isCQError = true;
		}
		throw new RocksDBException("put CQ failed");
	}

	private boolean putMessagePosition0() {
		if (!rocksDBStorage.hold()) {
			return false;
		}

		try {
			int size = bufferDRList.size();
			if (size == 0) {
				return true;
			}

			long maxPhyOffset = 0;
			for (int i = size - 1; i >= 0; i--) {
				DispatchRequest request = bufferDRList.get(i);
				byte[] topicBytes = request.getTopic().getBytes(DataConverter.CHARSET_UTF8);

				rocksDBConsumeQueueTable.buildAndPutCQByteBuffer(cqBBPairList.get(i), topicBytes, request, writeBatch);
				rocksDBCOnsumeQueueOffsetTable.updateTempTopicQueueMaxOffset(offsetBBPairList.get(i), topicBytes, request, tmpTopicQueueMaxOffsetMap);

				int msgSize = request.getMsgSize();
				long phyOffset = request.getCommitLogOffset();
				if (phyOffset + msgSize >= maxPhyOffset) {
					maxPhyOffset = phyOffset + msgSize;
				}
			}

			rocksDBCOnsumeQueueOffsetTable.putMaxPhyAndCqOffset(tmpTopicQueueMaxOffsetMap, writeBatch, maxPhyOffset);

			rocksDBStorage.batchPut(writeBatch);

			rocksDBCOnsumeQueueOffsetTable.putHeapMaxCqOffset(tmpTopicQueueMaxOffsetMap);

			long storeTimestamp = bufferDRList.get(size - 1).getStoreTimestamp();
			if (messageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE
					|| messageStore.getMessageStoreConfig().isEnableDLegerCommitLog()) {
				messageStore.getStoreCheckPoint().setPhysicMsgTimestamp(storeTimestamp);
			}
			messageStore.getStoreCheckPoint().setLogicsMsgTimestamp(storeTimestamp);

			notifyMessageArriveAndClear();
			return true;
		}
		catch (Exception e) {
			ERROR_LOG.error("putMessagePosition0 failed", e);
			return false;
		}
		finally {
			tmpTopicQueueMaxOffsetMap.clear();
			rocksDBStorage.release();
		}
	}

	private void notifyMessageArriveAndClear() {
		try {
			for (DispatchRequest dp : bufferDRList) {
				messageStore.notifyMessageArriveIfNecessary(dp);
			}
		}
		catch (Exception e) {
			ERROR_LOG.error("notifyMessageArriveAndClear failed", e);
		}
		finally {
			bufferDRList.clear();
		}
	}

	public Statistics getStatistics() {
		return rocksDBStorage.getStatistics();
	}

	@Override
	public List<ByteBuffer> rangeQuery(String topic, int queueId, long startIndex, int num) throws RocksDBException {
		return rocksDBConsumeQueueTable.rangeQuery(topic, queueId, startIndex, num);
	}

	@Override
	public ByteBuffer get(String topic, int queueId, long startIndex) throws RocksDBException {
		return rocksDBConsumeQueueTable.getCQInKV(topic ,queueId, startIndex);
	}

	@Override
	public void recoverOffsetTable(long minPhyOffset) {
		// NOP
	}

	@Override
	public void destroy() {
		try {
			shutdownInner();
			FileUtils.deleteDirectory(new File(storePath));
		}
		catch (Exception e) {
			ERROR_LOG.error("destroy cq Failed, {}", storePath, e);
		}
	}

	@Override
	public void destroy(ConsumeQueueInterface consumeQueue) throws RocksDBException {
		String topic = consumeQueue.getTopic();
		int queueId = consumeQueue.getQueueId();
		if (StringUtils.isEmpty(topic) || queueId < 0 || !rocksDBStorage.hold()) {
			return;
		}

		WriteBatch writeBatch = new WriteBatch();
		try {
			rocksDBConsumeQueueTable.destroyCQ(topic, queueId, writeBatch);
			rocksDBCOnsumeQueueOffsetTable.destroyOffset(topic, queueId, writeBatch);

			rocksDBStorage.batchPut(writeBatch);
		}
		catch (RocksDBException e) {
			ERROR_LOG.error("kv deleteTopic {} Failed.", topic, e);
			throw e;
		}
		finally {
			writeBatch.close();
			rocksDBStorage.release();
		}
	}

	@Override
	public boolean flush(ConsumeQueueInterface consumeQueue, int flushLeastPages) {
		try {
			rocksDBStorage.flushWAL();
		}
		catch (Exception ignored) {}
		return true;
	}

	@Override
	public void checkSelf() {
		// NOP
	}

	@Override
	public int deleteExpiredFile(ConsumeQueueInterface consumeQueue, long minCommitLogPos) {
		// NOP
		return 0;
	}

	@Override
	public void truncateDirty(long offsetToTruncate) throws RocksDBException {
		long maxPhyOffsetInRocksdb = getMaxPhyOffsetInConsumeQueue();
		if (offsetToTruncate >= maxPhyOffsetInRocksdb) {
			return;
		}

		rocksDBCOnsumeQueueOffsetTable.truncateDirty(offsetToTruncate);
	}

	@Override
	public void cleanExpired(long minPhyOffset) {
		rocksDBStorage.manualCompaction(minPhyOffset);
	}

	@Override
	public long getOffsetInQueueByTime(String topic, int queueId, long timestamp, BoundaryType boundaryType) throws RocksDBException {
		Long high = rocksDBCOnsumeQueueOffsetTable.getMaxCqOffset(topic, queueId);

		if (high == null || high == -1) {
			return 0;
		}

		long minPhyOffset = messageStore.getMinPhyOffset();
		long low = rocksDBCOnsumeQueueOffsetTable.getMinCqOffset(topic, queueId);

		return rocksDBConsumeQueueTable.binarySearchInCQByTime(topic, queueId, high, low, timestamp,
				minPhyOffset, boundaryType);
	}

	@Override
	public long getMaxOffsetInQueue(String topic, int queueId) throws RocksDBException {
		Long maxOffset = rocksDBCOnsumeQueueOffsetTable.getMaxCqOffset(topic, queueId);
		return maxOffset != null ? maxOffset + 1 : 0;
	}

	@Override
	public long getMinOffsetInQueue(String topic, int queueId) throws RocksDBException {
		return rocksDBCOnsumeQueueOffsetTable.getMinCqOffset(topic, queueId);
	}

	@Override
	public Long getMaxPhyOffsetInConsumeQueue(String topic, int queueId) {
		return rocksDBCOnsumeQueueOffsetTable.getMaxPhyOffset(topic ,queueId);
	}

	@Override
	public long getMaxPhyOffsetInConsumeQueue() throws RocksDBException {
		return rocksDBCOnsumeQueueOffsetTable.getMaxPhyOffset();
	}

	@Override
	public ConsumeQueueInterface findOrCreateConsumeQueue(String topic, int queueId) {
		ConcurrentMap<Integer, ConsumeQueueInterface> map = consumeQueueTable.computeIfAbsent(topic, k -> new ConcurrentHashMap<>(128));

		ConsumeQueueInterface logic = map.get(queueId);
		if (logic != null) {
			return logic;
		}

		return map.computeIfAbsent(queueId, k -> new RocksDBConsumeQueue(messageStore, topic, k));
	}

	@Override
	public long rollNextFile(ConsumeQueueInterface consumeQueue, long offset) {
		// NOP
		return 0;
	}

	@Override
	public boolean isFirstFileExist(ConsumeQueueInterface consumeQueue) {
		return true;
	}

	@Override
	public boolean isFirstFileAvailable(ConsumeQueueInterface consumeQueue) {
		return true;
	}

	@Override
	public long getTotalSize() {
		// NOP
		return 0;
	}
}
