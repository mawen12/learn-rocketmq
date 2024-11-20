package com.mawen.learn.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import com.google.common.hash.Hashing;
import com.mawen.learn.rocketmq.common.AbstractBrokerRunnable;
import com.mawen.learn.rocketmq.common.BrokerConfig;
import com.mawen.learn.rocketmq.common.BrokerIdentity;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.Pair;
import com.mawen.learn.rocketmq.common.ServiceThread;
import com.mawen.learn.rocketmq.common.SystemClock;
import com.mawen.learn.rocketmq.common.ThreadFactoryImpl;
import com.mawen.learn.rocketmq.common.TopicConfig;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.attribute.CQType;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.filter.MessageFilter;
import com.mawen.learn.rocketmq.common.message.MessageDecoder;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageExtBrokerInner;
import com.mawen.learn.rocketmq.common.sysflag.MessageSysFlag;
import com.mawen.learn.rocketmq.common.utils.ThreadUtils;
import com.mawen.learn.rocketmq.remoting.protocol.body.HARuntimeInfo;
import com.mawen.learn.rocketmq.store.config.BrokerRole;
import com.mawen.learn.rocketmq.store.config.FlushDiskType;
import com.mawen.learn.rocketmq.store.config.MessageStoreConfig;
import com.mawen.learn.rocketmq.store.config.StorePathConfigHelper;
import com.mawen.learn.rocketmq.store.ha.HAService;
import com.mawen.learn.rocketmq.store.ha.autoswitch.AutoSwitchHAService;
import com.mawen.learn.rocketmq.store.hook.PutMessageHook;
import com.mawen.learn.rocketmq.store.hook.SendMessageBackHook;
import com.mawen.learn.rocketmq.store.index.IndexService;
import com.mawen.learn.rocketmq.store.kv.CompactionService;
import com.mawen.learn.rocketmq.store.logfile.MappedFile;
import com.mawen.learn.rocketmq.store.metrics.DefaultStoreMetricsManager;
import com.mawen.learn.rocketmq.store.queue.ConsumeQueueInterface;
import com.mawen.learn.rocketmq.store.queue.ConsumeQueueStoreInterface;
import com.mawen.learn.rocketmq.store.queue.CqUnit;
import com.mawen.learn.rocketmq.store.stats.BrokerStatsManager;
import com.mawen.learn.rocketmq.store.timer.TimerMessageStore;
import com.mawen.learn.rocketmq.store.util.PerfCounter;
import io.netty.buffer.ByteBuf;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.ViewBuilder;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.RocksDBException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/6
 */
@Slf4j
@Getter
@Setter
public class DefaultMessageStore implements MessageStore {

	protected static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
	protected static final Logger ERROR_LOG = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

	private final PerfCounter.Ticks perfs = new PerfCounter.Ticks(LOGGER);

	private final MessageStoreConfig messageStoreConfig;

	protected final CommitLog commitLog;

	protected final ConsumeQueueStoreInterface consumeQueueStore;

	protected final FlushConsumeQueueService flushConsumeQueueService;

	protected final CleanCommitLogService cleanCommitLogService;

	protected final CleanConsumeQueueService cleanConsumeQueueService;

	private final CorrectLogicOffsetService correctLogicOffsetService;

	private final IndexService indexService;

	private final AllocateMappedFileService allocateMappedFileService;

	private ReputMessageService reputMessageService;

	private HAService haService;

	// CompactionLog
	private CompactionService compactionStore;

	private CompactionService compactionService;

	private final StoreStatService storeStatService;

	private final TransientStorePool transientStorePool;

	protected final RunningFlags runningFlags = new RunningFlags();
	private final SystemClock systemClock = new SystemClock();

	private final ScheduledExecutorService scheduledExecutorService;
	private final BrokerStatsManager brokerStatsManager;
	private final MessageArrivingListener messageArrivingListener;
	private final BrokerConfig brokerConfig;

	private volatile boolean shutdown = true;
	protected boolean notifyMessageArriveInBatch = false;

	private StoreCheckPoint storeCheckPoint;
	private TimerMessageStore timerMessageStore;

	private final LinkedList<CommitLogDispatcher> dispatcherList;

	private RandomAccessFile lockFile;

	private FileLock lock;

	boolean shutDownNormal = false;

	private final static int MAX_PULL_MSG_SIZE = 128 * 1024 * 1024;

	private volatile int aliveReplicasNum = 1;

	private MessageStore masterStoreInProcess;

	private volatile long masterFlushedOffset = -1L;

	private volatile long brokerInitMaxOffset = -1L;

	private List<PutMessageHook> putMessagehookList = new ArrayList<>();

	private SendMessageBackHook sendMessageBackHook;

	private final ConcurrentSkipListMap<Integer, Long> delayLevelTable = new ConcurrentSkipListMap<>();

	private int maxDelayLevel;

	private final AtomicInteger mappedPageHoldCount = new AtomicInteger(0);

	private final ConcurrentLinkedQueue<BatchDispatchRequest> batchDispatchRequestQueue = new ConcurrentLinkedQueue<BatchDispatchRequest>();

	private int dispatchRequestOrderlyQueueSize = 16;

	private final DispatchRequestOrderlyQueue dispatchRequestOrderlyQueue = new DispatchRequestOrderlyQueue(dispatchRequestOrderlyQueueSize);

	private long stateMachineVersion = 0L;

	private ConcurrentMap<String, TopicConfig> topicConfigTable;

	private final ScheduledExecutorService scheduledCleanQueueExecutorService = ThreadUtils.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StoreCleanQueueScheduledThread"));

	public MappedFile<String, Long> getMessageIds() {

	}

	@Override
	public boolean checkInDiskByConsumeOffset(String topic, int queueId, long consumeOffset) {
		long maxOffsetPy = commitLog.getMaxOffset();
		ConsumeQueueInterface consumeQueue = getConsumeQueue(topic, queueId);
		if (consumeQueue != null) {
			CqUnit cqUnit = consumeQueue.get(consumeOffset);
			if (cqUnit != null) {
				long offsetPy = cqUnit.getPos();
				return !estimateInMemByCommitOffset(offsetPy, maxOffsetPy);
			}
		}
		return false;
	}

	@Override
	public boolean checkInMemByConsumeOffset(String topic, int queueId, long consumeOffset, int batchSize) {
		ConsumeQueueInterface consumeQueue = getConsumeQueue(topic, queueId);
		if (consumeQueue != null) {
			CqUnit firstCQItem = consumeQueue.get(consumeOffset);
			if (firstCQItem == null) {
				return false;
			}

			long startOffsetPy = firstCQItem.getPos();
			if (batchSize <= 1) {
				int size = firstCQItem.getSize();
				return checkInMemByCommitOffset(startOffsetPy, size);
			}

			CqUnit lastCQItem = consumeQueue.get(consumeOffset + batchSize);
			if (lastCQItem == null) {
				int size = firstCQItem.getSize();
				return checkInMemByCommitOffset(startOffsetPy, size);
			}

			long endOffsetPy = lastCQItem.getPos();
			int size = (int) (endOffsetPy - startOffsetPy) + lastCQItem.getSize();
			return checkInMemByCommitOffset(startOffsetPy, size);
		}
		return false;
	}

	@Override
	public boolean checkInStoreByConsumeOffset(String topic, int queueId, long consumeOffset) {
		long commitLogOffset = getCommitLogOffsetInQueue(topic, queueId, consumeOffset);
		return checkInDiskByCommitOffset(commitLogOffset);
	}

	@Override
	public long dispatchBehindBytes() {
		return reputMessageService.behind();
	}

	public long flushBehindBytes() {
		if (messageStoreConfig.isTransientStorePoolEnable()) {
			return commitLog.remainHowManyDataToCommit() + commitLog.remainHowManyDataToFlush();
		}
		else {
			return commitLog.remainHowManyDataToFlush();
		}
	}

	@Override
	public long flush() {
		return commitLog.flush();
	}

	@Override
	public long getFlushedWhere() {
		return commitLog.getFlushedWhere();
	}

	@Override
	public boolean resetWriteOffset(long phyOffset) {
		ConcurrentMap<String, Long> newMap = new ConcurrentHashMap<>(consumeQueueStore.getTopicQueueTable());
		SelectMappedBufferResult lastBuffer = null;
		long startReadOffset = phyOffset == -1 ? 0 : phyOffset;
		while ((lastBuffer = selectOneMessageByOffset(startReadOffset)) != null) {
			try {
				if (lastBuffer.getStartOffset() > startReadOffset) {
					startReadOffset = lastBuffer.getStartOffset();
					continue;
				}

				ByteBuffer bb = lastBuffer.getByteBuffer();
				int magicCode = bb.getInt(bb.position() + 4);
				if (magicCode == CommitLog.BLANK_MAGIC_CODE) {
					startReadOffset += bb.getInt(bb.position());
					continue;
				}
				else if (magicCode != MessageDecoder.MESSAGE_MAGIC_CODE) {
					throw new RuntimeException("Unknown magicCode: " + magicCode);
				}

				lastBuffer.getByteBuffer().mark();

				DispatchRequest request = checkMessageAndReturnSize(lastBuffer.getByteBuffer(), true, messageStoreConfig.isDuplicationEnable(), true);
				if (!request.isSuccess()) {
					break;
				}

				lastBuffer.getByteBuffer().reset();

				MessageExt msg = MessageDecoder.decode(lastBuffer.getByteBuffer(), true, false, false, false, true);
				if (msg == null) {
					break;
				}
				String key = msg.getTopic() + "-" + msg.getQueueId();
				Long cur = newMap.get(key);
				if (cur != null && cur > msg.getQueueOffset()) {
					newMap.put(key, msg.getQueueOffset());
				}
				startReadOffset += msg.getStoreSize();
			}
			catch (Throwable e) {
				log.error("resetWriteOffset error.", e);
			}
			finally {
				if (lastBuffer != null) {
					lastBuffer.release();
				}
			}
		}

		if (commitLog.resetOffset(phyOffset)) {
			consumeQueueStore.setTopicQueueTable(newMap);
			return true;
		}
		return false;
	}

	@Override
	public long getConfirmOffset() {
		return commitLog.getConfirmOffset();
	}

	public long getConfirmOffsetDirectly() {
		return commitLog.getConfirmOffsetDirectly();
	}

	@Override
	public void setConfirmOffset(long phyOffset) {
		commitLog.setConfirmOffset(phyOffset);
	}

	@Override
	public byte[] calcDeltaChecksum(long from, long to) {
		if (from < 0 || to <= from) {
			return new byte[0];
		}

		int size = to - from;

		if (size > messageStoreConfig.getMaxChecksumRange()) {
			log.error("Checksum range from {}, size {} exceeds threshold {}", from, size, messageStoreConfig.getMaxChecksumRange());
			return null;
		}

		List<MessageExt> msgList = new ArrayList<>();
		List<SelectMappedBufferResult> bufferResultList = getBulkCommitLogData(from, size);
		if (bufferResultList.isEmpty()) {
			return new byte[0];
		}

		for (SelectMappedBufferResult bufferResult : bufferResultList) {
			msgList.addAll(MessageDecoder.decodeBatch(bufferResult.getByteBuffer(), true, false, false));
			bufferResult.release();
		}

		if (msgList.isEmpty()) {
			return new byte[0];
		}

		ByteBuffer buffer = ByteBuffer.allocate(size);
		for (MessageExt msg : msgList) {
			try {
				buffer.put(MessageDecoder.encodeUniquely(msg, false));
			}
			catch (IOException ignored) {}
		}

		return Hashing.murmur3_128().hashBytes(buffer.array()).asBytes();
	}

	@Override
	public void setPhysicalOffset(long phyOffset) {
		commitLog.setMappedFileQueueOffset(phyOffset)
	}

	@Override
	public boolean isMappedFilesEmpty() {
		return commitLog.isMappedFilesEmpty();
	}

	@Override
	public MessageExt lookMessageByOffset(long commitLogOffset, int size) {
		SelectMappedBufferResult result = commitLog.getMessage(commitLogOffset, size);
		if (result != null) {
			try {
				return MessageDecoder.decode(result.getByteBuffer(), true, false);
			}
			finally {
				result.release();
			}
		}
		return null;
	}

	@Override
	public ConsumeQueueInterface findConsumeQueue(String topic, int queueId) {
		return consumeQueueStore.findOrCreateConsumeQueue(topic, queueId);
	}

	private long nextOffsetCorrection(long oldOffset, long newOffset) {
		long nextOffset = oldOffset;
		if (getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE || getMessageStoreConfig().isOffsetCheckInSlave()) {
			nextOffset = newOffset;
		}
		return nextOffset;
	}

	private boolean estimateInMemByCommitOffset(long oldOffset, long newOffset) {
		long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
		return (maxOffsetPy - offsetPy) > memory;
	}

	private boolean checkInMemByCommitOffset(long offsetPy, int size) {
		SelectMappedBufferResult message = commitLog.getMessage(offsetPy, size);
		if (message != null) {
			try {
				return message.isInMem();
			}
			finally {
				message.release();
			}
		}
		return false;
	}

	public boolean checkInDiskByCommitOffset(long offsetPy) {
		return offsetPy >= commitLog.getMinOffset();
	}

	public boolean checkInColdAreaByCommitOffset(long offsetPy, long maxOffsetPy) {
		long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE + (messageStoreConfig.getAccessMessageInMemoryHotRatio() / 100.0));
		return (maxOffsetPy - offsetPy) > memory;
	}

	private boolean isTheBatchFull(int sizeBy, int unitBatchNum, int maxMsgNums, long maxMsgSize, int bufferTotal, int messageTotal, boolean isInMem) {
		if (bufferTotal == 0 || messageTotal == 0) {
			return false;
		}
		if (messageTotal + unitBatchNum > maxMsgNums) {
			return true;
		}
		if (bufferTotal + sizeBy > maxMsgSize) {
			return true;
		}

		if (isInMem) {
			if ((bufferTotal + sizeBy) > messageStoreConfig.getMaxTransferBytesOnMessageInMemory()) {
				return true;
			}

			return messageTotal > messageStoreConfig.getMaxTransferCountOnMessageInMemory() - 1;
		}
		else {
			if ((bufferTotal + sizeBy) > messageStoreConfig.getMaxTransferBytesOnMessageInDisk()) {
				return true;
			}

			return messageTotal > messageStoreConfig.getMaxTransferCountOnMessageInDisk() - 1;
		}
	}

	private void deleteFile(String fileName) {
		File file = new File(fileName);
		boolean result = file.delete();
		log.info("{} {}", fileName, result ? "create OK" : "already failed");
	}

	private void createTempFile() {
		String fileName = StorePathConfigHelper.getAbortFile(messageStoreConfig.getStorePathRootDir());
		File file = new File(fileName);
		UtilAll.ensureDirOK(file.getParent());
		boolean result = file.createNewFile();
		log.info("{} {}", fileName, result ? "create OK" : "already exists");
		MixAll.string2File(Long.toString(MixAll.getPID()), file.getAbsolutePath());
	}

	private void addScheduleTask() {
		scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
			@Override
			public void run0() {
				DefaultMessageStore.this.cleanFilesPeriodically();
			}
		}, 60 * 1000, messageStoreConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);

		scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
			@Override
			public void run0() {
				DefaultMessageStore.this.checkSelf();
			}
		}, 1, 10, TimeUnit.MINUTES);

		scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
			@Override
			public void run0() {
				if (DefaultMessageStore.this.getMessageStoreConfig().isDebugLockEnable()) {
					try {
						if (DefaultMessageStore.this.commitLog.getBeginTimeInLock() != 0) {
							long lockTime = System.currentTimeMillis() - DefaultMessageStore.this.commitLog.getBeginTimeInLock();
							if (lockTime > 1000 && lockTime < 10_000_000) {
								String stack = UtilAll.jstack();
								String fileName = System.getProperty("user.home") + File.separator + "debug/lock/stack-" + DefaultMessageStore.this.commitLog.getBeginTimeInLock() + "-" + lockTime;
								MixAll.string2FileNotSafe(stack, fileName);
							}
						}
					}
					catch (Exception e) {}
				}
			}
		}, 1, 1, TimeUnit.SECONDS);

		scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
			@Override
			public void run0() {
				DefaultMessageStore.this.storeCheckPoint.flush();
			}
		}, 1, 1, TimeUnit.SECONDS);

		scheduledCleanQueueExecutorService.scheduleAtFixedRate(() -> {
			DefaultMessageStore.this.cleanQueueFilesPeriodically();
		}, 60 * 1000, messageStoreConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);
	}

	private void cleanFilesPeriodically() {
		cleanCommitLogService.run();
	}

	private void cleanQueueFilesPeriodically() {
		correctLogicOffsetService.run();
		cleanConsumeQueueService.run();
	}

	private void checkSelf() {
		commitLog.checkSelf();
		consumeQueueStore.checkSelf();
	}

	private boolean isTempFileExist() {
		String fileName = StorePathConfigHelper.getAbortFile(messageStoreConfig.getStorePathRootDir());
		File file = new File(fileName);
		return file.exists();
	}

	private boolean isRecoverConcurrently() {
		return brokerConfig.isRecoveryConcurrently() && !messageStoreConfig.isEnableRocksDBStore();
	}

	private void recover(final boolean lastExitOK) {
		boolean recoverConcurrently = isRecoverConcurrently();
		log.info("message store recover mode: {}", recoverConcurrently ? "concurrent" : "normal");

		long recoverConsumeQueueStart = System.currentTimeMillis();
		recoverConsumeQueue();
		long maxPhyOffsetOfConsumeQueue = consumeQueueStore.getMaxPhyOffsetInConsumeQueue();
		long recoverConsumeQueueEnd = System.currentTimeMillis();

		if (lastExitOK) {
			commitLog.recoverNormally(maxPhyOffsetOfConsumeQueue);
		}
		else {
			commitLog.recoverAbnormally(maxPhyOffsetOfConsumeQueue);
		}

		long recoverCommitLogEnd = System.currentTimeMillis();
		recoverTopicQueueTable();
		long recoverConsumeOffsetEnd = System.currentTimeMillis();

		log.info("message store recover total cost: {}ms, recoverConsumeQueue: {}ms, recoverCommitLog: {}ms, recoverOffsetTable: {}ms",
				recoverConsumeOffsetEnd - recoverConsumeQueueStart, recoverConsumeQueueEnd - recoverConsumeQueueStart,
				recoverCommitLogEnd - recoverConsumeQueueEnd, recoverConsumeOffsetEnd - recoverCommitLogEnd);
	}

	@Override
	public long getTimingMessageCount(String topic) {
		if (timerMessageStore == null) {
			return 0L;
		}
		else {
			return timerMessageStore.getTimerMetrics().getTimingCount(topic);
		}
	}

	@Override
	public void finishCommitLogDispatch() {
		// NOP
	}

	private void recoverConsumeQueue() {
		if (!isRecoveryConcurrently()) {
			consumeQueueStore.recover();
		}
		else {
			consumeQueueStore.recoverConcurrently();
		}
	}

	@Override
	public void recoverTopicQueueTable() {
		long minPhyOffset = commitLog.getMinOffset();
		consumeQueueStore.recoverOffsetTable(minPhyOffset);
	}

	public RunningFlags getAccessRights() {
		return runningFlags;
	}

	public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> getConsumeQueueTable() {
		return consumeQueueStore.getConsumeQueueTable();
	}

	public void doDispatch(DispatchRequest request) throws RocksDBException {
		for (CommitLogDispatcher dispatcher : dispatcherList) {
			dispatcher.dispatch(request);
		}
	}

	protected void putMessagePositionInfo(DispatchRequest request) throws RocksDBException{
		consumeQueueStore.putMessagePositionInfoWrapper(request);
	}

	@Override
	public DispatchRequest checkMessageAndReturnSize(ByteBuffer buffer, boolean checkCRC, boolean checkDupInfo, boolean readBody) {
		return commitLog.checkMessageAndReturnSize(buffer, checkCRC, checkDupInfo, readBody);
	}

	@Override
	public boolean isTransientStorePoolDeficient() {
		return remainTransientStoreBufferNumbs() == 0;
	}

	@Override
	public int remainTransientStoreBufferNumbs() {
		return isTransientStorePoolEnable() ? transientStorePool.availableBufferNums() : Integer.MAX_VALUE;
	}

	@Override
	public long remainHowManyDataToCommit() {
		return commitLog.remainHowManyDataToCommit();
	}

	@Override
	public long remainHowManyDataToFlush() {
		return commitLog.remainHowManyDataToFlush();
	}

	@Override
	public void addDispatcher(CommitLogDispatcher dispatcher) {
		dispatcherList.add(dispatcher);
	}

	@Override
	public boolean getData(long offset, int size, ByteBuffer buffer) {
		return commitLog.getData(offset, size, buffer);
	}

	@Override
	public ConsumeQueueInterface getConsumeQueue(String topic, int queueId) {
		ConcurrentMap<Integer, ConsumeQueueInterface> map = getConsumeQueueTable().get(topic);
		return map == null ? null : map.get(queueId);
	}

	@Override
	public void unlockMappedFile(MappedFile unlockMappedFile) {
		scheduledExecutorService.schedule(() -> {
			unlockMappedFile.munlock();
		}, 6, TimeUnit.SECONDS);
	}

	@Override
	public PerfCounter.Ticks getPerfCounter() {
		return perfs;
	}

	@Override
	public ConsumeQueueStoreInterface getQueueStore() {
		return consumeQueueStore;
	}

	@Override
	public void onCommitLogDispatch(DispatchRequest request, boolean doDispatch, MappedFile commitLogFile, boolean isRecover, boolean isFileEnd) throws RocksDBException {
		if (doDispatch && !isFileEnd) {
			doDispatch(request);
		}
	}

	@Override
	public void onCommitLogAppend(MessageExtBrokerInner msg, AppendMessageResult result, MappedFile commitLogFile) {
		// NOP
	}

	@Override
	public boolean isSyncDiskFlush() {
		return FlushDiskType.SYNC_FLUSH == getMessageStoreConfig().getFlushDiskType();
	}

	@Override
	public boolean isSyncMaster() {
		return BrokerRole.SYNC_MASTER == getMessageStoreConfig().getBrokerRole();
	}

	@Override
	public void assignOffset(MessageExtBrokerInner msg) throws RocksDBException {
		int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());

		if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
			consumeQueueStore.assignQueueOffset(msg);
		}
	}

	@Override
	public void increaseOffset(MessageExtBrokerInner msg, short messageNum) {
		int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
		if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
			consumeQueueStore.increaseQueueOffset(msg,messageNum);
		}
	}

	@Override
	public void notifyMessageArriveIfNecessary(DispatchRequest request) {
		if (DefaultMessageStore.this.brokerConfig.isLongPollingEnable()
		    && DefaultMessageStore.this.messageArrivingListener != null) {
			DefaultMessageStore.this.messageArrivingListener.arriving(request.getTopic(), request.getQueueId(), request.getConsumeQueueOffset() + 1,
					request.getTagsCode(), request.getStoreTimestamp(), request.getBitMap(), request.getPropertiesMap());
			DefaultMessageStore.this.reputMessageService.notifyMessageArrive4MultiQueue(request);
		}
	}

	@Override
	public HARuntimeInfo getHARuntimeInfo() {
		if (haService != null) {
			return haService.getRuntimeInfo(commitLog.getMaxOffset());
		}
		return null;
	}

	public long computeDeliverTimestamp(int delayLevel, long storeTimestamp) {
		Long time = delayLevelTable.get(delayLevel);
		if (time != null) {
			return time + storeTimestamp;
		}

		return storeTimestamp + 1000;
	}

	@Override
	public long estimateMessageCount(String topic, int queueId, long from, long to, MessageFilter filter) {
		if (from < 0) {
			from = 0;
		}

		if (from >= to) {
			return 0;
		}

		if (filter == null) {
			return to - from;
		}

		ConsumeQueueInterface consumeQueue = findConsumeQueue(topic, queueId);
		if (consumeQueue == null) {
			return 0;
		}

		long minOffset = consumeQueue.getMinOffsetInQueue();
		if (from < minOffset) {
			long diff = to - from;
			from = minOffset;
			to = from + diff;
		}

		long msgCount = consumeQueue.estimateMessageCount(from, to, filter);
		return msgCount == -1 ? to - from : msgCount;
	}

	@Override
	public List<Pair<InstrumentSelector, ViewBuilder>> getMetricsView() {
		return DefaultStoreMetricsManager.getMetricsView();
	}

	@Override
	public void initMetrics(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier) {
		DefaultStoreMetricsManager.init(meter,attributesBuilderSupplier,this);
	}

	public boolean isTransientStorePoolEnable() {
		return messageStoreConfig.isTransientStorePoolEnable() &&
		       (brokerConfig.isEnableControllerMode() || messageStoreConfig.getBrokerRole() != BrokerRole.SLAVE);
	}

	public long getReputFromOffset() {
		return reputMessageService.getReputFromOffset();
	}

	public Optional<TopicConfig> getTopicConfig(String topic) {
		if (topicConfigTable == null) {
			return Optional.empty();
		}
		return Optional.ofNullable(topicConfigTable.get(topic));
	}

	public BrokerIdentity getBrokerIdentity() {
		if (messageStoreConfig.isEnableDLegerCommitLog()) {
			return new BrokerIdentity(brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(),
					Integer.parseInt(messageStoreConfig.getDLegerSelfId().substring(1)), brokerConfig.isInBrokerContainer());
		}
		else {
			return new BrokerIdentity(
					brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(),
					brokerConfig.getBrokerId(), brokerConfig.isInBrokerContainer()
			);
		}
	}

	class CommitLogDispatcherBuildConsumeQueue implements CommitLogDispatcher {

		@Override
		public void dispatch(DispatchRequest request) throws RocksDBException {
			int tranType = MessageSysFlag.getTransactionValue(request.getSysFlag());
			switch (tranType) {
				case MessageSysFlag.TRANSACTION_NOT_TYPE:
				case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
					putMessagePositionInfo(request);
					break;
				case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
				case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
					break;
			}
		}
	}

	class CommitLogDispatcherBuildIndex implements CommitLogDispatcher {
		@Override
		public void dispatch(DispatchRequest request) throws RocksDBException {
			if (DefaultMessageStore.this.messageStoreConfig.isMessageIndexEnable()) {
				DefaultMessageStore.this.indexService.buildIndex(request);
			}
		}
	}

	class FlushConsumeQueueService extends ServiceThread {
		private static final int RETRY_TIMES_OVER = 3;
		private long lastFlushTimestamp = 0;

		@Override
		public String getServiceName() {
			if (brokerConfig.isInBrokerContainer()) {
				return getBrokerIdentity().getIdentifier() + FlushConsumeQueueService.class.getSimpleName();
			}
			return FlushConsumeQueueService.class.getSimpleName();
		}

		@Override
		public void run() {
			String serviceName = getServiceName();
			LOGGER.info("{} service started", serviceName);

			while (!isStopped()) {
				try {
					int interval = getMessageStoreConfig().getFlushIntervalConsumeQueue();
					waitForRunning(interval);
					doFlush(1);
				}
				catch (Exception e) {
					LOGGER.warn("{} service has exception.", serviceName, e);
				}
			}

			LOGGER.info("{} service end", serviceName);
		}

		private void doFlush(int retryTimes) {
			int flushConsumeQueueLeastPages = getMessageStoreConfig().getFlushConsumeQueueLeastPages();
			if (retryTimes == RETRY_TIMES_OVER) {
				flushConsumeQueueLeastPages = 0;
			}

			long logicsMsgTimestamp = 0;
			int flushConsumeQueueThroughInterval = getMessageStoreConfig().getFlushConsumeQueueThroughInterval();
			long currentTimeMillis = System.currentTimeMillis();
			if (currentTimeMillis >= (lastFlushTimestamp + flushConsumeQueueThroughInterval)) {
				lastFlushTimestamp = currentTimeMillis;
				flushConsumeQueueLeastPages = 0;
				logicsMsgTimestamp = getStoreCheckPoint().getLogicsMsgTimestamp();
			}

			ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> tables = getConsumeQueueTable();
			for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : tables.values()) {
				for (ConsumeQueueInterface cq : maps.values()) {
					boolean result = false;
					for (int i = 0; i < retryTimes && !result; i++) {
						result = consumeQueueStore.flush(cq, flushConsumeQueueLeastPages);
					}
				}
			}

			if (messageStoreConfig.isEnableCompaction()) {
				compactionStore.flush(flushConsumeQueueLeastPages);
			}

			if (flushConsumeQueueLeastPages == 0) {
				getStoreCheckPoint().setLogicsMsgTimestamp(logicsMsgTimestamp);
			}
			getStoreCheckPoint().flush();
		}

		@Override
		public long getJoinTime() {
			return 60 * 1000;
		}
	}

	@Getter
	@AllArgsConstructor
	class BatchDispatchRequest {
		private ByteBuffer buffer;
		private int position;
		private int size;
		private long id;
	}

	class DispatchRequestOrderlyQueue {
		DispatchRequest[][] buffer;

		long ptr = 0;

		AtomicLong maxPtr = new AtomicLong();

		public DispatchRequestOrderlyQueue(int bufferNum) {
			buffer = new DispatchRequest[bufferNum][];
		}

		public void put(long index, DispatchRequest[] dispatchRequests) {
			while (ptr + buffer.length <= index) {
				synchronized (this) {
					try {
						this.wait();
					}
					catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				}
			}

			int mod = (int) (index % buffer.length);
			this.buffer[mod] = dispatchRequests;
			maxPtr.incrementAndGet();
		}

		public DispatchRequest[] get(List<DispatchRequest[]> dispatchRequestList) {
			synchronized (this) {
				for (int i = 0; i < buffer.length; i++) {
					int mod = (int) (ptr % buffer.length);
					DispatchRequest[] ret = buffer[mod];
					if (ret == null) {
						this.notifyAll();
						return null;
					}
					dispatchRequestList.add(ret);
					buffer[mod] = null;
					ptr++;
				}
			}
			return null;
		}

		public synchronized boolean isEmpty() {
			return maxPtr.get() == ptr;
		}
	}

	@Getter
	@Setter
	class ReputMessageService extends ServiceThread {

		protected volatile long reputFromOffset = 0;

		@Override
		public String getServiceName() {
			return "";
		}

		@Override
		public void run() {

		}

		@Override
		public void shutdown() {
			for (int i = 0; i < 50 && this.isCommitLogAvailable(); i++) {
				try {
					Thread.sleep(100);
				}
				catch (InterruptedException ignored) {}
			}

			if (this.isCommitLogAvailable()) {
				LOGGER.warn("shutdown ReputMessageService, but CommitLog have no finish to be dispatched, CommitLog max offset = {}, reputFromOffset = {}",
						commitLog.getMaxOffset(), reputFromOffset);
			}

			super.shutdown();
		}

		public long behind() {
			return getConfirmOffset() - reputFromOffset;
		}

		public boolean isCommitLogAvailable() {
			return reputFromOffset < getConfirmOffset();
		}

		public void doReput() {
			if (reputFromOffset < commitLog.getMinOffset()) {
				LOGGER.warn("The reputFromOffset = {} is smaller than minPyOffset={}, " +
						"this usually indicate that the dispatch behind too much and the commit log has expired.",
						reputFromOffset, commitLog.getMinOffset());
				reputFromOffset = commitLog.getMinOffset();
			}

			for (boolean doNext = true; isCommitLogAvailable() && doNext;) {
				commitLog.getData(reputFromOffset);
			}
		}
	}

	class CleanCommitLogService {
		private static final int MAX_MANUAL_DELETE_FILE_TIMES = 20;
		private final String diskSpaceWarningLevelRatio = System.getProperty("rocketmq.broker.diskSpaceWarningLevelRatio", "");
		private final String diskSpaceCleanForciblyRatio = System.getProperty("rocketmq.broker.diskSpaceCleanForciblyRatio", "");

		private long lastReDeleteTimestamp = 0;
		private final AtomicInteger manualDeleteFileSeveralTimes = new AtomicInteger();
		private volatile boolean cleanImmediately = false;
		private int forceCleanFailedTimes = 0;

		double getDiskSpaceWarningLevelRatio() {
			double finalDiskSpaceWarningLevelRatio ;
			if ("".equals(diskSpaceWarningLevelRatio)) {
				finalDiskSpaceWarningLevelRatio = DefaultMessageStore.this.getMessageStoreConfig().getDiskSpaceWarningLevelRatio();
			}
			else {
				finalDiskSpaceWarningLevelRatio = Double.parseDouble(diskSpaceWarningLevelRatio);
			}

			if (finalDiskSpaceWarningLevelRatio > 0.90) {
				finalDiskSpaceWarningLevelRatio = 0.90;
			}

			if (finalDiskSpaceWarningLevelRatio < 0.35) {
				finalDiskSpaceWarningLevelRatio = 0.35;
			}

			return finalDiskSpaceWarningLevelRatio;
		}

		double getDiskSpaceCleanForciblyRatio() {
			double finalDiskSpaceCleanForciblyRatio;
			if ("".equals(diskSpaceCleanForciblyRatio)) {
				finalDiskSpaceCleanForciblyRatio = DefaultMessageStore.this.getMessageStoreConfig().getDiskSpaceCleanForciblyRatio();
			}
			else {
				finalDiskSpaceCleanForciblyRatio = Double.parseDouble(diskSpaceCleanForciblyRatio);
			}

			if (finalDiskSpaceCleanForciblyRatio > 0.85) {
				finalDiskSpaceCleanForciblyRatio = 0.85;
			}

			if (finalDiskSpaceCleanForciblyRatio < 0.30) {
				finalDiskSpaceCleanForciblyRatio = 0.30;
			}

			return finalDiskSpaceCleanForciblyRatio;
		}

		public void executeDeleteFilesManually() {
			this.manualDeleteFileSeveralTimes.set(MAX_MANUAL_DELETE_FILE_TIMES);
			log.info("executeDeleteFilesManually was invoked");
		}

		public void run() {
			try {
				deleteExpiredFiles();
				reDeleteHangedFile();
			}
			catch (Throwable e) {
				log.warn("{} service has exception", getServiceName(), e);
			}
		}

		private void deleteExpiredFiles(){
			int deleteCount = 0;
			long fileReservedTime = DefaultMessageStore.this.getMessageStoreConfig().getFileReservedTime();
			int deletePhysicFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteCommitLogFilesInterval();
			int destroyMappedFileIntervalForcibly = DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
			int deleteFileBatchMax = DefaultMessageStore.this.getMessageStoreConfig().getDeleteFileBatchMax();

			boolean isTimeUp = this.isTimeToDelete();
			boolean isUsageExceedsThreshold = this.isSpaceToDelete();
			boolean isManualDelete = this.manualDeleteFileSeveralTimes.get() > 0;

			if (isTimeUp || isUsageExceedsThreshold || isManualDelete) {
				if (isManualDelete) {
					this.manualDeleteFileSeveralTimes.decrementAndGet();
				}

				boolean cleanAtOnce = DefaultMessageStore.this.getMessageStoreConfig().isCleanFileForciblyEnable() && this.cleanImmediately;

				log.info("begin to delete before {} hours file. isTimeUp: {} isUsageExceedsThreshold: {} manualDeleteFileSeveralTimes: {} cleanAtOnce: {} deleteFileBatchMax: {}",
						fileReservedTime, isTimeUp, isUsageExceedsThreshold, manualDeleteFileSeveralTimes.get(), cleanAtOnce, deleteFileBatchMax);

				fileReservedTime *= 60 * 60 * 1000;

				deleteCount = DefaultMessageStore.this.commitLog.deleteExpiredFile(fileReservedTime, deletePhysicFilesInterval, destroyMappedFileIntervalForcibly, cleanAtOnce, deleteFileBatchMax);
				if (deleteCount > 0) {
					if (DefaultMessageStore.this.brokerConfig.isEnableControllerMode()) {
						if (DefaultMessageStore.this.haService instanceof AutoSwitchHAService) {
							final long minPhyOffset = getMinPhyOffset();
							((AutoSwitchHAService)DefaultMessageStore.this.haService).truncateEpochFilePrefix(minPhyOffset - 1);
						}
					}
				}
				else if (isUsageExceedsThreshold) {
					log.warn("disk space will be full soon, but delete file failed.");
				}
			}
		}

		private void reDeleteHangedFile() {
			int interval = DefaultMessageStore.this.getMessageStoreConfig().getRedeleteHangedFileInterval();
			long currentTimestamp = System.currentTimeMillis();
			if ((currentTimestamp - this.lastReDeleteTimestamp) > interval) {
				this.lastReDeleteTimestamp = currentTimestamp;
				int destroyMappedFileIntervalForcibly = DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
				if (DefaultMessageStore.this.commitLog.retryDeleteFirstFile(destroyMappedFileIntervalForcibly)) {
				}
			}
		}

		public String getServiceName() {
			return DefaultMessageStore.this.brokerConfig.getIdentifier() + CleanCommitLogService.class.getSimpleName();
		}

		protected boolean isTimeToDelete() {
			String when = DefaultMessageStore.this.getMessageStoreConfig().getDeleteWhen();
			if (UtilAll.isItTimeToDo(when)) {
				log.info("it's time to reclaim disk space, {}", when);
				return true;
			}
			return false;
		}

		private boolean isSpaceToDelete() {
			cleanImmediately = false;

			String commitLogStorePath = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
			String[] storePaths = commitLogStorePath.trim().split(MixAll.MULTI_PATH_SPLITTER);
			Set<String> fullStorePath = new HashSet<>();
			double minPhysicalRatio = 100;
			String minStorePath = null;
			for (String storePathPhysic : storePaths) {
				double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
				if (minPhysicalRatio > physicRatio) {
					minPhysicalRatio = physicRatio;
					minStorePath = storePathPhysic;
				}
				if (physicRatio > getDiskSpaceCleanForciblyRatio()) {
					fullStorePath.add(storePathPhysic);
				}
			}

			DefaultMessageStore.this.commitLog.setFullStorePaths(fullStorePath);
			if (minPhysicalRatio > getDiskSpaceWarningLevelRatio()) {
				boolean diskFull = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
				if (diskFull) {
					log.error("physic disk maybe full soon {}, so mark disk full, storePathPhysic={}", minPhysicalRatio, minStorePath);
				}

				cleanImmediately = true;
				return true;
			}
			else if (minPhysicalRatio > getDiskSpaceCleanForciblyRatio()) {
				cleanImmediately = true;
				return true;
			}
			else {
				boolean diskOK = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
				if (!diskOK) {
					log.info("physic disk space OK {}, so mark disk ok, storePathPhysic={}", minPhysicalRatio, minStorePath);
				}
			}

			String storePathLogics = StorePathConfigHelper.getStorePathConsumeQueue(DefaultMessageStore.this.getMessageStoreConfig().getStorePathRootDir());
			double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
			if (logicsRatio > getDiskSpaceWarningLevelRatio()) {
				boolean diskOK = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
				if (diskOK) {
					log.error("logics disk maybe full soon {}, so mark disk full", logicsRatio);
				}
				cleanImmediately = true;
				return true;
			}
			else if (logicsRatio > getDiskSpaceCleanForciblyRatio()) {
				cleanImmediately = true;
				return true;
			}
			else {
				boolean diskOK = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
				if (!diskOK) {
					log.info("logics disk space OK {}, so mark disk ok", logicsRatio);
				}
			}

			double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;
			int replicasPerPartition = DefaultMessageStore.this.getMessageStoreConfig().getReplicasPerDiskPartition();
			if (replicasPerPartition <= 1) {
				if (minPhysicalRatio < 0 || minPhysicalRatio > ratio) {
					log.info("commitLog disk maybe full soon, so reclaim space, {}", minPhysicalRatio);
					return true;
				}

				if (logicsRatio < 0 || logicsRatio > ratio) {
					log.info("consumeQueue disk maybe full soon, so reclaim space, {}", logicsRatio);
					return true;
				}
				return false;
			}
			else {
				long majorFileSize = DefaultMessageStore.this.getMajorFileSize();
				long partitionLogicalSize = UtilAll.getDiskPartitionTotalSpace(minStorePath) / replicasPerPartition;
				double logicalRatio = 1.0 * majorFileSize / partitionLogicalSize;

				if (logicalRatio > DefaultMessageStore.this.getMessageStoreConfig().getLogicDiskSpaceCleanForciblyThreshold()) {
					log.info("Logical disk space {} exceeds logical disk space clean forcibly threshold {}, forcibly: {}",
							logicalRatio, minPhysicalRatio, cleanImmediately);
					cleanImmediately = true;
					return true;
				}

				boolean isUsageExceedsThreshold = logicalRatio > ratio;
				if (isUsageExceedsThreshold) {
					log.info("Logical disk usage {} exceeds clean threshold {}, forcibly: {}",
							logicalRatio, ratio, cleanImmediately);
				}
				return isUsageExceedsThreshold;
			}
		}

		public int getManualDeleteFileSeveralTimes() {
			return manualDeleteFileSeveralTimes.get();
		}

		public void setManualDeleteFileSeveralTimes(int manualDeleteFileSeveralTimes) {
			this.manualDeleteFileSeveralTimes.set(manualDeleteFileSeveralTimes);
		}

		public double calcStorePathPhysicRatio() {
			Set<String> fullStorePath = new HashSet<>();
			String storePath = getStorePathPhysic();
			String[] paths = storePath.trim().split(MixAll.MULTI_PATH_SPLITTER);
			double minPhysicRatio = 100;
			for (String path : paths) {
				double physicRatio = UtilAll.isPathExists(path) ? UtilAll.getDiskPartitionSpaceUsedPercent(path) : -1;
				minPhysicRatio = Math.min(minPhysicRatio, physicRatio);
				if (physicRatio > getDiskSpaceCleanForciblyRatio()) {
					fullStorePath.add(path);
				}
			}
			DefaultMessageStore.this.commitLog.setFullStorePaths(fullStorePath);
			return minPhysicRatio;
		}

		public boolean isSpaceFull() {
			double physicRatio = calcStorePathPhysicRatio();
			double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;
			if (physicRatio > ratio) {
				log.info("physic disk of commitLog used: {}", physicRatio);
			}

			if (physicRatio > getDiskSpaceWarningLevelRatio()) {
				boolean diskFull = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
				if (diskFull) {
					log.error("physic disk of commitLog maybe full soon, used {}, so mark disk full", physicRatio);
				}
				return true;
			}
			else {
				boolean diskOK = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
				if (!diskOK) {
					log.info("physic disk space of commitLog OK {}, so mark disk ok", physicRatio);
				}

				return false;
			}
		}
	}

	class CleanConsumeQueueService {
		protected long lastPhysicalMinOffset = 0;

		public void run() {
			try {
				deleteExpiredFiles();
			}
			catch (Throwable e) {
				log.warn("{} service has exception.", getServiceName(), e);
			}
		}

		protected void deleteExpiredFiles() {
			int deleteLogicsFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteConsumeQueueFilesInterval();
			long minOffset = DefaultMessageStore.this.commitLog.getMinOffset();
			if (minOffset > lastPhysicalMinOffset) {
				lastPhysicalMinOffset = minOffset;

				ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> tables = DefaultMessageStore.this.getQueueStore().getConsumeQueueTable();

				for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : tables.values()) {
					for (ConsumeQueueInterface logic : maps.values()) {
						int deleteCount = DefaultMessageStore.this.consumeQueueStore.deleteExpiredFile(logic, minOffset);
						if (deleteCount > 0 && deleteLogicsFilesInterval > 0) {
							try {
								Thread.sleep(deleteLogicsFilesInterval);
							}
							catch (InterruptedException ignored) {
							}
						}
					}

					DefaultMessageStore.this.indexService.deleteExpiredFile(minOffset);
				}
			}
		}

		public String getServiceName() {
			return DefaultMessageStore.this.brokerConfig.getIdentifier() + CleanConsumeQueueService.class.getSimpleName();
		}
	}

	class CorrectLogicOffsetService {
		private long lastForceCorrectTime = -1L;

		public void run() {
			try {
				correctLogicMinOffset();
			}
			catch (Throwable e) {
				log.warn("{} service has exception.", getServiceName(), e);
			}
		}

		private boolean needCorrect(ConsumeQueueInterface logic, long minPhyOffset, long lastForeCorrectTimeCurRun) {
			if (logic == null) {
				return false;
			}

			if (DefaultMessageStore.this.consumeQueueStore.isFirstFileExist(logic) && !DefaultMessageStore.this.consumeQueueStore.isFirstFileAvailable(logic)) {
				log.error("CorrectLogOffsetService.needCorrect. first file not available, trigger correct. topic:{}, queue:{}, maxPhyOffset in queue:{}, minPhyOffset in commit log:{}, minOffset in queue:{}, maxOffset in queue:{}, cqType:{}",
						logic.getMaxPhysicOffset(), minPhyOffset, logic.getMinOffsetInQueue(), logic.getMaxOffsetInQueue(), logic.getTopic(), logic.getQueueId(), logic.getCQType());
				return true;
			}

			if (logic.getMaxPhysicOffset() == -1 || minPhyOffset == -1) {
				return false;
			}

			if (logic.getMaxPhysicOffset() < minPhyOffset) {
				if (logic.getMinOffsetInQueue() < logic.getMaxOffsetInQueue()) {
					log.error("CorrectLogicOffsetService.needCorrect. logic max phy offset: {} is less than min phy offset: {}, but min offset: {} is less than max offset: {}. topic:{}, queue:{}, cqType:{}.",
							logic.getMaxPhysicOffset(), minPhyOffset, logic.getMinOffsetInQueue(), logic.getMaxOffsetInQueue(), logic.getTopic(), logic.getQueueId(), logic.getCQType());
					return true;
				}
				else if (logic.getMinOffsetInQueue() == logic.getMaxOffsetInQueue()) {
					return false;
				}
				else {
					log.error("CorrectLogicOffsetService.needCorrect. It should not happen, logic max phy offset: {} is less than min phy offset: {}, but min offset: {} is larger than max offset: {}, topic: {}, queue: {}, cqType: {}",
							logic.getMaxPhysicOffset(), minPhyOffset, logic.getMinOffsetInQueue(), logic.getMaxOffsetInQueue(), logic.getTopic(), logic.getQueueId(), logic.getCQType());
					return false;
				}
			}

			int forceCorrectInterval = DefaultMessageStore.this.getMessageStoreConfig().getCorrectLogicMinOffsetForceInterval();
			if ((System.currentTimeMillis() - lastForeCorrectTimeCurRun) > forceCorrectInterval) {
				lastForceCorrectTime = System.currentTimeMillis();
				CqUnit cqUnit = logic.getEarliestUnit();
				if (cqUnit == null) {
					if (logic.getMinOffsetInQueue() == logic.getMaxOffsetInQueue()) {
						return false;
					}
					else {
						log.error("CorrectLogicOffsetService.needCorrect. cqUnit is null, logic max phy offset: {} is greater than min phy offset: {}, but min offset: {} is not equal to max offset: {}, topic: {}, queue: {}, cqType: {}",
								logic.getMaxPhysicOffset(), minPhyOffset, logic.getMinOffsetInQueue(), logic.getMaxOffsetInQueue(), logic.getTopic(), logic.getQueueId(), logic.getCQType());
						return true;
					}
				}

				if (cqUnit.getPos() < minPhyOffset) {
					log.error("CorrectLogicOffsetService.needCorrect. logic max phy offset: {} is greater than min phy offset: {}, but minPhyPos in cq is: {}. min offset in queue: {}, max offset in queue: {}, topic:{}, queue:{}, cqType:{}.",
							logic.getMaxOffsetInQueue(), minPhyOffset, cqUnit.getPos(), logic.getMinOffsetInQueue(), logic.getMaxOffsetInQueue(), logic.getTopic(), logic.getQueueId(), logic.getCQType());
					return true;
				}

				if (cqUnit.getPos() >= minPhyOffset) {
					return false;
				}
			}
			return false;
		}

		private void correctLogicMinOffset() {
			long lastForeCorrectTimeCurRun = lastForceCorrectTime;
			long minPhyOffset = getMinPhyOffset();
			ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> tables = DefaultMessageStore.this.getConsumeQueueTable();
			for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : tables.values()) {
				for (ConsumeQueueInterface logic : maps.values()) {
					if (Objects.equals(CQType.SimpleCQ, logic.getCQType())) {
						continue;
					}
					if (needCorrect(logic, minPhyOffset, lastForeCorrectTimeCurRun)) {
						doCorrect(logic, minPhyOffset);
					}
				}
			}
		}

		private void doCorrect(ConsumeQueueInterface logic, long minPhyOffset) {
			DefaultMessageStore.this.consumeQueueStore.deleteExpiredFile(logic, minPhyOffset);
			int sleepIntervalWhenCorrectMinOffset = DefaultMessageStore.this.getMessageStoreConfig().getCorrectLogicMinOffsetSleepInterval();
			if (sleepIntervalWhenCorrectMinOffset > 0) {
				try {
					Thread.sleep(sleepIntervalWhenCorrectMinOffset);
				}
				catch (InterruptedException ignored) {}
			}
		}

		public String getServiceName() {
			if (brokerConfig.isInBrokerContainer()) {
				return brokerConfig.getIdentifier() + CorrectLogicOffsetService.class.getSimpleName();
			}
			return CorrectLogicOffsetService.class.getSimpleName();
		}
	}

	class FlushConsumeQueueService extends ServiceThread {
		private static final int RETRY_TIMES_OVER = 3;
		private long lastFlushTimestamp = 0;

		private void doFlush(int retryTimes) {
			int flushConsumeQueueLeastPages = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueLeastPages();
			if (retryTimes == RETRY_TIMES_OVER) {
				flushConsumeQueueLeastPages = 0;
			}

			long logicMsgTimestamp = 0;
			int flushConsumeQueueThoroughInterval = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueThroughInterval();
			long currentTimeMillis = System.currentTimeMillis();
			if (currentTimeMillis >= (lastFlushTimestamp + flushConsumeQueueThoroughInterval)) {
				lastFlushTimestamp = currentTimeMillis;
				flushConsumeQueueLeastPages = 0;
				logicMsgTimestamp = DefaultMessageStore.this.getStoreCheckPoint().getLogicsMsgTimestamp();
			}

			ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> tables = DefaultMessageStore.this.getQueueStore().getConsumeQueueTable();
			for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : tables.values()) {
				for (ConsumeQueueInterface cq : maps.values()) {
					boolean result = false;
					for (int i = 0; i < retryTimes && !result; i++) {
						result = DefaultMessageStore.this.consumeQueueStore.flush(cq, flushConsumeQueueLeastPages);
					}
				}
			}

			if (messageStoreConfig.isEnableCompaction()) {
				compactionStore.flush(flushConsumeQueueLeastPages);
			}

			if (0 == flushConsumeQueueLeastPages) {
				if (logicMsgTimestamp > 0) {
					DefaultMessageStore.this.getStoreCheckPoint().setLogicsMsgTimestamp(logicMsgTimestamp);
				}
				DefaultMessageStore.this.getStoreCheckPoint().flush();
			}
		}

		@Override
		public void run() {
			String serviceName = getServiceName();
			log.info("{} service started", serviceName);

			while (!isStopped()) {
				try {
					int interval = DefaultMessageStore.this.getMessageStoreConfig().getFlushIntervalConsumeQueue();
					waitForRunning(interval);
					doFlush(1);
				}
				catch (Exception e) {
					log.warn("{} service has exception.", serviceName, e);
				}
			}

			doFlush(RETRY_TIMES_OVER);

			log.info("{} service end", serviceName);
		}

		@Override
		public String getServiceName() {
			if (DefaultMessageStore.this.brokerConfig.isInBrokerContainer()) {
				return DefaultMessageStore.this.getBrokerIdentify().getIdentifier() + FlushConsumeQueueService.class.getSimpleName();
			}
			return FlushConsumeQueueService.class.getSimpleName();
		}

		@Override
		public long getJoinTime() {
			return 60 * 1000;
		}
	}

	class MainBatchDispatchRequestService extends ServiceThread {

		private final ExecutorService batchDispatchRequestExecutor;

		public MainBatchDispatchRequestService() {
			batchDispatchRequestExecutor = ThreadUtils.newThreadPoolExecutor(
					DefaultMessageStore.this.getMessageStoreConfig().getBatchDispatchRequestThreadPoolNums(),
					DefaultMessageStore.this.getMessageStoreConfig().getBatchDispatchRequestThreadPoolNums(),
					60 * 1000,
					TimeUnit.MICROSECONDS,
					new LinkedBlockingDeque<>(4096),
					new ThreadFactoryImpl("BatchDispatchRequestServiceThread_"),
					new ThreadPoolExecutor.AbortPolicy());
		}

		private void pollBatchDispatchRequest() {
			try {
				if (!batchDispatchRequestQueue.isEmpty()) {
					BatchDispatchRequest task = batchDispatchRequestQueue.peek();
					batchDispatchRequestExecutor.execute(() -> {
						try {
							ByteBuffer tempByteBuffer = task.buffer;
							tempByteBuffer.position(task.position);
							tempByteBuffer.limit(task.position + task.size);

							List<DispatchRequest> dispatchRequestList = new ArrayList<>();
							while (tempByteBuffer.hasRemaining()) {
								DispatchRequest dispatchRequest = DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(tempByteBuffer, false, false, false);
								if (dispatchRequest.isSuccess()) {
									dispatchRequestList.add(dispatchRequest);
								}
								else {
									log.error("[BUG]read total count not equals msg total size.");
								}
							}
							dispatchRequestOrderlyQueue.put(task.id, dispatchRequestList.toArray(new DispatchRequest[dispatchRequestList.size()]));
							mappedPageHoldCount.getAndDecrement();
						}
						catch (Exception e) {
							log.error("There is an exception in task execution.", e);
						}
					});

					batchDispatchRequestQueue.poll();
				}
			}
			catch (Exception e) {
				log.warn("{} service has exception.", getServiceName(), e);
			}
		}

		@Override
		public void run() {
			String serviceName = getServiceName();
			log.info("{} service started", serviceName);

			while (!isStopped()) {
				try {
					TimeUnit.MILLISECONDS.sleep(1);
					pollBatchDispatchRequest();
				}
				catch (Exception e) {
					log.warn("{} service has exception.", serviceName, e);
				}
			}

			log.info("{} service end", serviceName);
		}

		@Override
		public String getServiceName() {
			if (DefaultMessageStore.this.getBrokerConfig().isInBrokerContainer()) {
				return DefaultMessageStore.this.getBrokerIdentity().getIdentifier() + MainBatchDispatchRequestService.class.getSimpleName();
			}
			return MainBatchDispatchRequestService.class.getSimpleName();
		}
	}

	class DispatchService extends ServiceThread {

		private final List<DispatchRequest[]> dispatchRequestsList = new ArrayList<>();

		public void dispatch() {
			dispatchRequestsList.clear();
			dispatchRequestOrderlyQueue.get(dispatchRequestsList);
			if (!dispatchRequestsList.isEmpty()) {
				for (DispatchRequest[] dispatchRequests : dispatchRequestsList) {
					for (DispatchRequest dispatchRequest : dispatchRequests) {
						DefaultMessageStore.this.doDispatch(dispatchRequest);
						DefaultMessageStore.this.notifyMessageArriveIfNecessary(dispatchRequest);

						if (!DefaultMessageStore.this.getMessageStoreConfig().isDuplicationEnable() &&
						    DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
							DefaultMessageStore.this.storeStatService.getSinglePutMessageTopicTimesTotal(dispatchRequest.getTopic()).add(1);
							DefaultMessageStore.this.storeStatService.getSinglePutMessageTopicTimesTotal(dispatchRequest.getTopic()).add(dispatchRequest.getMsgSize());
						}
					}
				}
			}
		}

		@Override
		public void run() {
			String serviceName = getServiceName();
			log.info("{} service started", serviceName);

			while (!isStopped()) {
				try {
					TimeUnit.MILLISECONDS.sleep(1);
					dispatch();
				}
				catch (Exception e) {
					log.warn("{} service has exception.", serviceName, e);
				}
			}

			log.info("{} service end", serviceName);
		}

		@Override
		public String getServiceName() {
			if (DefaultMessageStore.this.getBrokerConfig().isInBrokerContainer()) {
				return DefaultMessageStore.this.getBrokerIdentity().getIdentifier() + DispatchService.class.getSimpleName();
			}
			return DispatchService.class.getSimpleName();
		}
	}

	class ConcurrentReputMessageService extends ReputMessageService {
		private static final int BATCH_SIZE = 4 * 1024 * 1024;

		private long batchId = 0;

		private MainBatchDispatchRequestService mainBatchDispatchRequestService;

		private DispatchService dispatchService;

		public ConcurrentReputMessageService() {
			super();
			this.mainBatchDispatchRequestService = new MainBatchDispatchRequestService();
			this.dispatchService = new DispatchService();
		}

		public void createBatchDispatchRequest(ByteBuffer buffer, int position, int size) {
			if (position < 0) {
				return;
			}

			mappedPageHoldCount.getAndIncrement();
			BatchDispatchRequest task = new BatchDispatchRequest(buffer.duplicate(), position, size, batchId++);
			batchDispatchRequestQueue.offer(task);
		}

		@Override
		public void start() {
			super.start();
			mainBatchDispatchRequestService.start();
			dispatchService.start();
		}

		@Override
		public void doReput() {
			if (reputFromOffset < DefaultMessageStore.this.commitLog.getMinOffset()) {
				log.warn("The reputFromOffset={} is smaller than minPhyOffset={}, this usually indicate that the dispatch behind too much and the commitLog has expired.",
						reputFromOffset ,DefaultMessageStore.this.commitLog.getMinOffset());
				reputFromOffset = DefaultMessageStore.this.commitLog.getMinOffset();
			}

			for (boolean doNext = true; isCommitLogAvailable() && doNext; ) {
				SelectMappedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);
				if (result == null) {
					break;
				}

				int batchDispatchRequestStart = -1;
				int batchDispatchRequestSize = -1;
				try {
					reputFromOffset = result.getStartOffset();

					for (int readSize = 0; readSize < result.getSize() && reputFromOffset < getReputFromOffset() && doNext; ) {
						ByteBuffer buffer = result.getByteBuffer();
						int totalSize = preCheckMessageAndReturnSize(buffer);
						if (totalSize > 0) {
							if (batchDispatchRequestStart == -1) {
								batchDispatchRequestStart = buffer.position();
								batchDispatchRequestSize = 0;
							}
							batchDispatchRequestSize += totalSize;
							if (batchDispatchRequestSize > BATCH_SIZE) {
								createBatchDispatchRequest(buffer, batchDispatchRequestStart, batchDispatchRequestSize);
								batchDispatchRequestStart = -1;
								batchDispatchRequestSize = -1;
							}
							buffer.position(buffer.position() + totalSize);
							reputFromOffset += totalSize;
							readSize += totalSize;
						}
						else {
							doNext = false;
							if (totalSize == 0) {
								reputFromOffset = DefaultMessageStore.this.commitLog.rollNextFile(reputFromOffset);
							}
							createBatchDispatchRequest(buffer, batchDispatchRequestStart, batchDispatchRequestSize);
							batchDispatchRequestStart = -1;
							batchDispatchRequestSize = -1;
						}
					}
				}
				finally {
					createBatchDispatchRequest(result.getByteBuffer(), batchDispatchRequestStart, batchDispatchRequestSize);
					boolean over = mappedPageHoldCount.get() == 0;
					while (!over) {
						try {
							TimeUnit.MILLISECONDS.sleep(1);
						}
						catch (Exception e) {
							e.printStackTrace();
						}
						over = mappedPageHoldCount.get() == 0;
					}
					result.release();
				}
			}

			finishCommitLogDispatch();
		}

		public int preCheckMessageAndReturnSize(ByteBuffer buffer) {
			buffer.mark();

			int totalSize = buffer.getInt();
			if (reputFromOffset + totalSize > DefaultMessageStore.this.getConfirmOffset()) {
				return -1;
			}

			int magicCode = buffer.getInt();
			switch (magicCode) {
				case MessageDecoder.MESSAGE_MAGIC_CODE:
				case MessageDecoder.MESSAGE_MAGIC_CODE_V2:
					break;
				case MessageDecoder.BLANK_MAGIC_CODE:
					return 0;
				default:
					return -1;
			}

			buffer.reset();

			return totalSize;
		}

		@Override
		public void shutdown() {
			for (int i = 0; i < 50 && isCommitLogAvailable(); i++) {
				try {
					TimeUnit.MILLISECONDS.sleep(100);
				}
				catch (InterruptedException ignored) {
				}
			}

			if (isCommitLogAvailable()) {
				log.warn("shutdown concurrentReputMessageService, but CommitLog have not finish to be dispatched, CommitLog max offset={}, reputFromOffset={}",
						DefaultMessageStore.this.commitLog.getMaxOffset(), reputFromOffset);
			}

			mainBatchDispatchRequestService.shutdown();
			dispatchService.shutdown();
			super.shutdown();
		}

		@Override
		public String getServiceName() {
			if (DefaultMessageStore.this.getBrokerConfig().isInBrokerContainer()) {
				return DefaultMessageStore.this.getBrokerIdentify().getIdentifer() + ConcurrentReputMessageService.class.getSimpleName();
			}
			return ConcurrentReputMessageService.class.getSimpleName();
		}
	}
}
