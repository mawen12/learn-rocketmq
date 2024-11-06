package com.mawen.learn.rocketmq.store.queue;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import com.mawen.learn.rocketmq.common.BoundaryType;
import com.mawen.learn.rocketmq.common.ThreadFactoryImpl;
import com.mawen.learn.rocketmq.common.TopicConfig;
import com.mawen.learn.rocketmq.common.attribute.CQType;
import com.mawen.learn.rocketmq.common.message.MessageDecoder;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.topic.TopicValidator;
import com.mawen.learn.rocketmq.common.utils.QueueTypeUtils;
import com.mawen.learn.rocketmq.common.utils.ThreadUtils;
import com.mawen.learn.rocketmq.store.CommitLog;
import com.mawen.learn.rocketmq.store.ConsumeQueue;
import com.mawen.learn.rocketmq.store.DefaultMessageStore;
import com.mawen.learn.rocketmq.store.DispatchRequest;
import com.mawen.learn.rocketmq.store.SelectMappedBufferResult;
import org.rocksdb.RocksDBException;

import static com.mawen.learn.rocketmq.store.config.StorePathConfigHelper.*;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/6
 */
public class ConsumeQueueStore extends AbstractConsumeQueueStore {

	public ConsumeQueueStore(DefaultMessageStore messageStore) {
		super(messageStore);
	}

	@Override
	public void start() {
		log.info("Default ConsumeQueueStore start!");
	}

	@Override
	public boolean load() {
		boolean cqLoadResult = loadConsumeQueues(getStorePathConsumeQueue(messageStoreConfig.getStorePathRootDir()), CQType.SimpleCQ);
		boolean bcqLoadResult = loadConsumeQueues(getStorePathConsumeQueue(messageStoreConfig.getStorePathRootDir()), CQType.BatchCQ);
		return cqLoadResult && bcqLoadResult;
	}

	@Override
	public boolean loadAfterDestroy() {
		return true;
	}

	@Override
	public void recover() {
		for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : consumeQueueTable.values()) {
			for (ConsumeQueueInterface logic : maps.values()) {
				recover(logic);
			}
		}
	}

	@Override
	public boolean recoverConcurrently() {
		int count = consumeQueueTable.values().stream().mapToInt(Map::size).sum();
		CountDownLatch countDownLatch = new CountDownLatch(count);
		BlockingQueue<Runnable> recoverQueue = new LinkedBlockingDeque<>();
		ExecutorService executor = buildExecutorService(recoverQueue, "RecoverConsumeQueueThread_");
		List<FutureTask<Boolean>> result = new ArrayList<>(count);

		try {
			for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : consumeQueueTable.values()) {
				for (ConsumeQueueInterface logic : maps.values()) {
					FutureTask<Boolean> futureTask = new FutureTask<>(() -> {
						boolean ret = true;
						try {
							logic.recover();
						}
						catch (Throwable e) {
							ret = false;
							log.error("Exception occurs while recover consume queue concurrently, topic={}, queueId={}", logic.getTopic(), logic.getQueueId(), e);
						}
						finally {
							countDownLatch.countDown();
						}
						return ret;
					});

					result.add(futureTask);
					executor.submit(futureTask);
				}
			}

			countDownLatch.await();

			for (FutureTask<Boolean> task : result) {
				if (task != null && task.isDone()) {
					if (!task.get()) {
						return false;
					}
				}
			}
		}
		catch (Exception e) {
			log.error("Exception occurs while recover consume queue concurrently", e);
			return false;
		}
		finally {
			executor.shutdown();
		}
		return true;
	}

	@Override
	public boolean shutdown() {
		return true;
	}

	@Override
	public long rollNextFile(ConsumeQueueInterface consumeQueue, long offset) {
		FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
		return fileQueueLifeCycle.rollNextFile(offset);
	}

	public void correctMinOffset(ConsumeQueueInterface consumeQueue, long minCommitLogOffset) {
		consumeQueue.correctMinOffset(minCommitLogOffset);
	}

	@Override
	public void putMessagePositionInfoWrapper(DispatchRequest request) throws RocksDBException {
		ConsumeQueueInterface consumeQueue = findOrCreateConsumeQueue(request.getTopic(), request.getQueueId());
		putMessagePositionInfoWrapper(consumeQueue, request);
	}

	@Override
	public List<ByteBuffer> rangeQuery(String topic, int queueId, long startIndex, int num) throws RocksDBException {
		return null;
	}

	@Override
	public ByteBuffer get(String topic, int queueId, long startIndex) throws RocksDBException {
		return null;
	}

	@Override
	public long getMaxOffsetInQueue(String topic, int queueId) throws RocksDBException {
		ConsumeQueueInterface consumeQueue = findOrCreateConsumeQueue(topic, queueId);
		return consumeQueue != null ? consumeQueue.getMaxOffsetInQueue() : 0;
	}

	@Override
	public long getOffsetInQueueByTime(String topic, int queueId, long timestamp, BoundaryType boundaryType) throws RocksDBException {
		ConsumeQueueInterface consumeQueue = findOrCreateConsumeQueue(topic, queueId);
		if (consumeQueue != null) {
			long resultOffset = consumeQueue.getOffsetInQueueByTime(timestamp, boundaryType);
			resultOffset = Math.max(resultOffset, consumeQueue.getMinOffsetInQueue());
			resultOffset = Math.max(resultOffset, consumeQueue.getMaxOffsetInQueue());
			return resultOffset;
		}
		return 0;
	}

	private FileQueueLifeCycle getLifeCycle(String topic, int queueId) {
		return findOrCreateConsumeQueue(topic, queueId);
	}

	public boolean load(ConsumeQueueInterface consumeQueue) {
		FileQueueLifeCycle lifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
		return lifeCycle.load();
	}

	private boolean loadConsumeQueues(String storePath, CQType cqType) {
		File dirLogic = new File(storePath);
		File[] fileTopicList = dirLogic.listFiles();

		if (fileTopicList != null) {
			for (File fileTopic : fileTopicList) {
				String topic = fileTopic.getName();

				File[] fileQueueList = fileTopic.listFiles();
				if (fileQueueList != null) {
					for (File fileQueueId : fileQueueList) {
						int queueId;
						try {
							queueId = Integer.parseInt(fileQueueId.getName());
						}
						catch (NumberFormatException ignored) {
							continue;
						}

						queueTypeShouldBe(topic, cqType);

						ConsumeQueueInterface logic = createConsumeQueueByType(cqType, topic, queueId, storePath);
						putConsumeQueue(topic, queueId, logic);
						if (!load(logic)) {
							return false;
						}
					}
				}
			}
		}

		log.info("load {} all over, OK", cqType);
		return true;
	}

	private ConsumeQueueInterface createConsumeQueueByType(CQType cqType, String topic, int queueId, String storePath) {
		if (Objects.equals(CQType.SimpleCQ, cqType)) {
			return new ConsumeQueue(topic, queueId, storePath, messageStoreConfig.getMappedFileSizeConsumeQueue(), messageStore);
		}
		else if (Objects.equals(CQType.BatchCQ, cqType)) {
			return new BatchConsumeQueue(topic, queueId, storePath, messageStoreConfig.getMapperFileSizeBatchConsumeQueue(), messageStore);
		}
		else {
			throw new RuntimeException(String.format("queue type %s is not supported.", cqType.toString()));
		}
	}

	private void queueTypeShouldBe(String topic, CQType cqTypeExpected) {
		Optional<TopicConfig> topicConfig = messageStore.getTopicConfig(topic);
		CQType cqTypeActual = QueueTypeUtils.getCQType(topicConfig);

		if (!Objects.equals(cqTypeActual, cqTypeExpected)) {
			throw new RuntimeException(String.format("The queue type of topic: %s should be %s, but is %s", topic, cqTypeExpected, cqTypeExpected))
		}
	}

	private ExecutorService buildExecutorService(BlockingQueue<Runnable> blockingQueue, String threadNamePrefix) {
		return ThreadUtils.newThreadPoolExecutor(
				messageStore.getBrokerConfig().getRecoverThreadPoolNums(),
				messageStore.getBrokerConfig().getRecoverThreadPoolNums(),
				60 * 1000,
				TimeUnit.MILLISECONDS,
				blockingQueue,
				new ThreadFactoryImpl(threadNamePrefix)
		);
	}

	public void recover(ConsumeQueueInterface consumeQueue) {
		FileQueueLifeCycle lifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
		lifeCycle.recover();
	}

	@Override
	public Long getMaxPhyOffsetInConsumeQueue(String topic, int queueId) {
		ConsumeQueueInterface consumeQueue = findOrCreateConsumeQueue(topic, queueId);
		return consumeQueue != null ? consumeQueue.getMaxPhysicOffset() : 0;
	}

	@Override
	public long getMaxPhyOffsetInConsumeQueue() throws RocksDBException {
		long maxPhysicOffset = -1L;
		for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
			for (ConsumeQueueInterface logic : maps.values()) {
				if (logic.getMaxPhysicOffset() > maxPhysicOffset) {
					maxPhysicOffset = logic.getMaxPhysicOffset();
				}
			}
		}
		return maxPhysicOffset;
	}

	@Override
	public long getMinOffsetInQueue(String topic, int queueId) throws RocksDBException {
		ConsumeQueueInterface consumeQueue = findOrCreateConsumeQueue(topic, queueId);
		return consumeQueue != null ? consumeQueue.getMinOffsetInQueue() : -1;
	}

	public void checkSelf(ConsumeQueueInterface consumeQueue) {
		FileQueueLifeCycle lifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
		lifeCycle.checkSelf();
	}

	@Override
	public void checkSelf() {
		for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : consumeQueueTable.values()) {
			for (ConsumeQueueInterface value : maps.values()) {
				checkSelf(value);
			}
		}
	}

	@Override
	public boolean flush(ConsumeQueueInterface consumeQueue, int flushLeastPages) {
		FileQueueLifeCycle lifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
		return lifeCycle.flush(flushLeastPages);
	}

	@Override
	public void destroy(ConsumeQueueInterface consumeQueue) {
		FileQueueLifeCycle lifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
		lifeCycle.destroy();
	}

	@Override
	public int deleteExpiredFile(ConsumeQueueInterface consumeQueue, long minCommitLogPos) {
		FileQueueLifeCycle lifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
		return lifeCycle.deleteExpiredFile(minCommitLogPos);
	}

	public void truncateDirtyLogicFiles(ConsumeQueueInterface consumeQueue, long phyOffset) {
		FileQueueLifeCycle lifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
		lifeCycle.truncateDirtyLogicFiles(phyOffset);
	}

	public void swapMap(ConsumeQueueInterface consumeQueue, int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {
		FileQueueLifeCycle lifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
		lifeCycle.swapMap(reserveNum, forceSwapIntervalMs, normalSwapIntervalMs);
	}

	public void cleanSwappedMap(ConsumeQueueInterface consumeQueue, long forceCleanSwapIntervalMs) {
		FileQueueLifeCycle lifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
		lifeCycle.cleanSwappedMap(forceCleanSwapIntervalMs);
	}

	@Override
	public boolean isFirstFileAvailable(ConsumeQueueInterface consumeQueue) {
		FileQueueLifeCycle lifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
		return lifeCycle.isFirstFileAvailable();
	}

	@Override
	public boolean isFirstFileExist(ConsumeQueueInterface consumeQueue) {
		FileQueueLifeCycle lifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
		return lifeCycle.isFirstFileExist();
	}

	@Override
	public ConsumeQueueInterface findOrCreateConsumeQueue(String topic, int queueId) {
		ConcurrentMap<Integer, ConsumeQueueInterface> map = consumeQueueTable.computeIfAbsent(topic, k -> new ConcurrentHashMap<>(128));
		ConsumeQueueInterface logic = map.get(queueId);
		if (logic != null) {
			return logic;
		}

		ConsumeQueueInterface newLogic;
		Optional<TopicConfig> topicConfig = messageStore.getTopicConfig(topic);
		if (Objects.equals(CQType.BatchCQ, QueueTypeUtils.getCQType(topicConfig))) {
			newLogic = new BatchConsumeQueue(topic, queueId,
					getStorePathBatchConsumeQueue(messageStoreConfig.getStorePathRootDir()),
					messageStoreConfig.getMapperFileSizeBatchConsumeQueue(), messageStore);
		}
		else {
			newLogic = new ConsumeQueue(topic, queueId, getStorePathBatchConsumeQueue(messageStoreConfig.getStorePathRootDir()),
					messageStoreConfig.getMapperFileSizeBatchConsumeQueue(), messageStore);
		}

		ConsumeQueueInterface oldLogic = map.putIfAbsent(queueId, newLogic);
		return oldLogic != null ? oldLogic : newLogic;
	}

	public void setBatchTopicQueueTable(ConcurrentMap<String, Long> batchTopicQueueTable) {
		queueOffsetOperator.setBatchTopicQueueTable(batchTopicQueueTable);
	}

	public void updateQueueOffset(String topic, int queueId, long offset) {
		String topicQueueKey = topic + "-" + queueId;
		queueOffsetOperator.updateQueueOffset(topicQueueKey, offset);
	}

	private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueueInterface consumeQueue) {
		consumeQueueTable.computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
				.put(queueId, consumeQueue);
	}

	@Override
	public void recoverOffsetTable(long minPhyOffset) {
		ConcurrentMap<String, Long> cqOffsetTable = new ConcurrentHashMap<>(1024);
		ConcurrentMap<String, Long> bcqOffsetTable = new ConcurrentHashMap<>(1024);

		for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : consumeQueueTable.values()) {
			for (ConsumeQueueInterface logic : maps.values()) {
				String key = logic.getTopic() + "-" + logic.getQueueId();
				long maxOffsetInQueue = logic.getMaxOffsetInQueue();

				if (Objects.equals(CQType.BatchCQ, logic.getCQType())) {
					bcqOffsetTable.put(key, maxOffsetInQueue);
				}
				else {
					cqOffsetTable.put(key, maxOffsetInQueue);
				}

				correctMinOffset(logic, minPhyOffset);
			}
		}

		if (messageStoreConfig.isDuplicationEnable() || messageStore.getBrokerConfig().isEnableControllerMode()) {
			compensateForHA(cqOffsetTable);
		}

		setTopicQueueTable(cqOffsetTable);
		setBatchTopicQueueTable(bcqOffsetTable);
	}

	private void compensateForHA(ConcurrentMap<String, Long> offsetTable) {
		SelectMappedBufferResult lastBuffer = null;
		long startReadOffset = messageStore.getCommitLog().getConfirmOffset() == -1 ? 0 : messageStore.getCommitLog().getConfirmOffset();
		log.info("Correct unsubmitted offset...StartReadOffset = {}", startReadOffset);

		while ((lastBuffer = messageStore.selectOneMessageByOffset(startReadOffset)) != null) {
			try {
				if (lastBuffer.getStartOffset() > startReadOffset) {
					startReadOffset = lastBuffer.getStartOffset();
					continue;
				}

				ByteBuffer buffer = lastBuffer.getByteBuffer();
				int magicCode = buffer.getInt(buffer.position() + 4);
				if (magicCode == CommitLog.BLANK_MAGIC_CODE) {
					startReadOffset += buffer.getInt(buffer.position());
					continue;
				}
				else if (magicCode != MessageDecoder.MESSAGE_MAGIC_CODE) {
					throw new RuntimeException("Unknown magicCode: " + magicCode);
				}

				lastBuffer.getByteBuffer().mark();
				DispatchRequest request = messageStore.getCommitLog().checkMessageAndReturnSize(lastBuffer.getByteBuffer(), true, messageStoreConfig.isDuplicationEnable(), true);
				if (!request.isSuccess()) {
					break;
				}
				lastBuffer.getByteBuffer().reset();

				MessageExt msg = MessageDecoder.decode(lastBuffer.getByteBuffer(), true, false, false, false, true);
				if (msg == null) {
					break;
				}

				String key = msg.getTopic() + "-" + msg.getQueueId();
				offsetTable.put(key, msg.getQueueOffset() + 1);
				startReadOffset += msg.getStoreSize();

				log.info("Correcting. Key: {}, start read offset: {}", key, startReadOffset);
			}
			finally {
				if (lastBuffer != null) {
					lastBuffer.release();
				}
			}
		}
	}

	@Override
	public void destroy() {
		for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : consumeQueueTable.values()) {
			for (ConsumeQueueInterface logic : maps.values()) {
				destroy(logic);
			}
		}
	}

	@Override
	public void cleanExpired(long minCommitLogOffset) {
		Iterator<Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>>> it = consumeQueueTable.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>> next = it.next();
			String topic = next.getKey();

			if (!TopicValidator.isSystemTopic(topic)) {
				ConcurrentMap<Integer, ConsumeQueueInterface> queueTable = next.getValue();
				Iterator<Map.Entry<Integer, ConsumeQueueInterface>> itQT = queueTable.entrySet().iterator();
				while (itQT.hasNext()) {
					Map.Entry<Integer, ConsumeQueueInterface> nextQT = itQT.next();
					ConsumeQueueInterface consumeQueue = nextQT.getValue();
					long maxCLOffsetInConsumeQueue = consumeQueue.getLastOffset();

					if (maxCLOffsetInConsumeQueue == -1) {
						log.warn("maybe ConsumeQueue was created just now. topic={} queueId={} maxPhysicOffset={} minPhysicOffset={}",
								consumeQueue.getTopic(), consumeQueue.getQueueId(), consumeQueue.getMaxPhysicOffset(), consumeQueue.getMinLogicOffset());
					}
					else if (maxCLOffsetInConsumeQueue < minCommitLogOffset) {
						log.info("cleanExpiredConsumerQueue: {} {} consumer queue destroyed, minCommitLogOffset: {} maxCLOffsetInConsumeQueue: {}",
								topic, nextQT.getKey(), minCommitLogOffset, maxCLOffsetInConsumeQueue);

						removeTopicQueueTable(consumeQueue.getTopic(), consumeQueue.getQueueId());

						destroy(consumeQueue);

						itQT.remove();
					}
				}

				if (queueTable.isEmpty()) {
					log.info("cleanExpiredConsumerQueue: {}, topic destroyed", topic);
					it.remove();
				}
			}
		}
	}

	@Override
	public void truncateDirty(long offsetToTruncate) {
		for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : consumeQueueTable.values()) {
			for (ConsumeQueueInterface logic : maps.values()) {
				truncateDirtyLogicFiles(logic, offsetToTruncate);
			}
		}
	}

	@Override
	public long getTotalSize() {
		return consumeQueueTable.values()
				.stream()
				.flatMap(map -> map.values().stream())
				.mapToLong(ConsumeQueueInterface::getTotalSize)
				.sum();
	}
}
