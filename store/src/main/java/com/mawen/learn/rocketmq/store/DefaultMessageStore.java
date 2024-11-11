package com.mawen.learn.rocketmq.store;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.mawen.learn.rocketmq.common.BrokerConfig;
import com.mawen.learn.rocketmq.common.ServiceThread;
import com.mawen.learn.rocketmq.common.SystemClock;
import com.mawen.learn.rocketmq.common.ThreadFactoryImpl;
import com.mawen.learn.rocketmq.common.TopicConfig;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.utils.ThreadUtils;
import com.mawen.learn.rocketmq.store.config.MessageStoreConfig;
import com.mawen.learn.rocketmq.store.ha.HAService;
import com.mawen.learn.rocketmq.store.hook.PutMessageHook;
import com.mawen.learn.rocketmq.store.hook.SendMessageBackHook;
import com.mawen.learn.rocketmq.store.queue.ConsumeQueueInterface;
import com.mawen.learn.rocketmq.store.queue.ConsumeQueueStoreInterface;
import com.mawen.learn.rocketmq.store.stats.BrokerStatsManager;
import com.mawen.learn.rocketmq.store.timer.TimerMessageStore;
import com.mawen.learn.rocketmq.store.util.PerfCounter;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/6
 */
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
		private int id;
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
}
