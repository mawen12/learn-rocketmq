package com.mawen.learn.rocketmq.store;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.mawen.learn.rocketmq.common.ServiceThread;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.store.config.FlushDiskType;
import lombok.Getter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/6
 */
@Getter
public class CommitLog implements Swappable {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	public static final int MESSAGE_MAGIC_CODE = -626843481;
	public static final int BLANK_MAGIC_CODE = -875286124;
	public static final int CRC32_RESERVED_LEN = MessageConst.PROPERTY_CRC32.length() + 1 + 10 + 1;

	protected final MappedFileQueue mappedFileQueue;
	protected final DefaultMessageStore defaultMessageStore;

	private final FlushManager flushManager;


	abstract class FlushCommitLogService extends ServiceThread {
		protected static final int RETRY_TIMES_OVER = 10;
	}

	class CommitRealTimeService extends FlushCommitLogService {
		private long lastCommitTimestamp = 0;

		@Override
		public String getServiceName() {
			if (CommitLog.this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
				return CommitLog.this.defaultMessageStore.getBrokerIdentity().getIdentifier() + CommitRealTimeService.class.getSimpleName();
			}
			return CommitRealTimeService.class.getSimpleName();
		}

		@Override
		public void run() {
			String serviceName = getServiceName();
			log.info("{} service started", serviceName);

			while (!isStopped()) {
				int interval = defaultMessageStore.getMessageStoreConfig().getCommitIntervalCommitLog();

				int commitDataLeastPages = defaultMessageStore.getMessageStoreConfig().getCommitCommitLogLeastPages();

				int commitDataThroughInterval = defaultMessageStore.getMessageStoreConfig().getCommitCommitLogThoroughInterval();

				long begin = System.currentTimeMillis();
				if (begin > (lastCommitTimestamp + commitDataThroughInterval)) {
					lastCommitTimestamp = begin;
					commitDataLeastPages = 0;
				}

				try {
					boolean result = mappedFileQueue.commit(commitDataLeastPages);
					long end = System.currentTimeMillis();
					if (!result) {
						lastCommitTimestamp = end;
						flushManager.wakeUpFlush();
					}
					getMessageStore().getPerfCounter().flowOnce("COMMIT_DATA_TIME_MS", (int) (end - begin));
					if (end - begin > 500) {
						log.info("Commit data to file costs {} ms", end - begin);
					}
					waitForRunning(interval);
				}
				catch (Throwable e) {
					log.error("{} service has exception.", serviceName, e);
				}
			}

			boolean result = false;
			for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
				result = mappedFileQueue.commit(0);
				log.info("{} service shutdown, retry {} times {}", serviceName, i + 1, result ? "OK" : "Not OK");
			}

			log.info("{} service end", serviceName);
		}
	}

	@Getter
	public static class GroupCommitRequest {
		private final long nextOffset;
		private final CompletableFuture<PutMessageStatus> flushOKFuture = new CompletableFuture<>();
		private volatile int ackNums = 1;
		private final long deadLine;

		public GroupCommitRequest(long nextOffset, long timeoutMillis) {
			this.nextOffset = nextOffset;
			this.deadLine = System.nanoTime() + (timeoutMillis * 1_000_000);
		}

		public GroupCommitRequest(long nextOffset, long timeoutMillis, int ackNums) {
			this(nextOffset, timeoutMillis);
			this.ackNums = ackNums;
		}

		public void wakeupCustomer(final PutMessageStatus status) {
			flushOKFuture.complete(status);
		}

		public CompletableFuture<PutMessageStatus> future() {
			return flushOKFuture;
		}
	}

	class GroupCommitService extends FlushCommitLogService {
		private volatile LinkedList<GroupCommitRequest> requestsWrite = new LinkedList<>();
		private volatile LinkedList<GroupCommitRequest> requestsRead = new LinkedList<>();
		private final PutMessageSpinLock lock = new PutMessageSpinLock();

		public void putRequest(final GroupCommitRequest request) {
			lock.lock();
			try {
				requestsWrite.add(request);
			}
			finally {
				lock.unlock();
			}
		}

		private void swapRequests() {
			lock.lock();
			try {
				LinkedList<GroupCommitRequest> tmp = this.requestsWrite;
				this.requestsWrite = this.requestsRead;
				this.requestsRead = tmp;
			}
			finally {
				lock.unlock();
			}
		}

		private void doCommit() {
			if (!requestsRead.isEmpty()) {
				for (GroupCommitRequest req : requestsRead) {
					boolean flushOK = mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
					for (int i = 0; i < 1000 && !flushOK; i++) {
						mappedFileQueue.flush(0);
						flushOK = mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
						if (flushOK) {
							break;
						}
						else {
							try {
								Thread.sleep(1);
							}
							catch (InterruptedException ignored) {}
						}
					}

					req.wakeupCustomer(flushOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
				}

				long storeTimestamp = mappedFileQueue.getStoreTimestamp();
				if (storeTimestamp > 0) {
					defaultMessageStore.getStoreCheckPoint().setPhysicMsgTimestamp(storeTimestamp);
				}

				requestsRead = new LinkedList<>();
			}
			else {
				mappedFileQueue.flush(0);
			}
		}

		@Override
		public String getServiceName() {
			if (defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
				return defaultMessageStore.getBrokerConfig().getIdentifier() + GroupCommitService.class.getSimpleName();
			}
			return GroupCommitService.class.getSimpleName();
		}

		@Override
		protected void onWaitEnd() {
			swapRequests();
		}

		@Override
		public void run() {
			String serviceName = getServiceName();
			log.info("{} service started", serviceName);

			while (!isStopped()) {
				try {
					waitForRunning(10);
					doCommit();
				}
				catch (Exception e) {
					log.warn("{} service has exception.", serviceName, e);
				}
			}

			try {
				Thread.sleep(10);
			}
			catch (InterruptedException e) {
				log.warn("GroupCommitService Exception.", e);
			}

			swapRequests();
			doCommit();

			log.info("{} service end", serviceName);
		}

		@Override
		public long getJoinTime() {
			return 5 * 60 * 1000;
		}
	}

	class FlushRealTimeService extends FlushCommitLogService {
		private long lastFlushTimestamp = 0;
		private long printTimes = 0;

		@Override
		public String getServiceName() {
			if (defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
				return defaultMessageStore.getBrokerConfig().getIdentifier() + FlushRealTimeService.class.getSimpleName();
			}
			return FlushRealTimeService.class.getSimpleName();
		}

		@Override
		public void run() {
			String serviceName = getServiceName();
			log.info("{} service started", serviceName);

			while (!isStopped()) {
				boolean flushCommitLogTimed = defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();

				int interval = defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();
				int flushPhysicQueueLeastPages = defaultMessageStore.getMessageStoreConfig().getFlushCommitLogLeastPages();

				int flushPhysicalQueueThoroughInterval = defaultMessageStore.getMessageStoreConfig().getFlushCommitLogThoroughInterval();

				boolean printFlushProgress = false;

				long current = System.currentTimeMillis();
				if (current >= (lastFlushTimestamp + flushPhysicalQueueThoroughInterval)) {
					lastFlushTimestamp = current;
					flushPhysicQueueLeastPages = 0;
					printFlushProgress = (printTimes++ % 10) == 0;
				}

				try {
					if (flushCommitLogTimed) {
						Thread.sleep(interval);
					}
					else {
						waitForRunning(interval);
					}

					if (printFlushProgress) {
						printFlushProgress();
					}

					long begin = System.currentTimeMillis();
					mappedFileQueue.flush(flushPhysicQueueLeastPages);
					long storeTimestamp = mappedFileQueue.getStoreTimestamp();
					if (storeTimestamp > 0) {
						defaultMessageStore.getStoreCheckPoint().setPhysicMsgTimestamp(storeTimestamp);
					}

					long past = System.currentTimeMillis() - begin;
					getMessageStore().getPerfCounter().flowOnce("FLUSH_DATA_TIME_MS", past);
					if (past > 500) {
						log.info("Flush data to disk costs {}ms", past);
					}
				}
				catch (Throwable e) {
					log.warn("{} service has exception.", serviceName, e);
					printFlushProgress();
				}
			}

			boolean result = false;
			for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
				result = mappedFileQueue.flush(0);
				log.info("{} service shutdown, retry {} times {}", serviceName, i + 1, result ? "OK" : "Not OK");
			}

			printFlushProgress();

			log.info("{} service end", serviceName);
		}

		@Override
		public long getJoinTime() {
			return 5 * 60 * 1000;
		}

		private void printFlushProgress() {

		}
	}

	class GroupCheckService extends FlushCommitLogService {
		private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<>();
		private volatile List<GroupCommitRequest> requestsRead = new ArrayList<>();

		public boolean isAsyncRequestsFull() {
			return requestsWrite.size() > defaultMessageStore.getMessageStoreConfig().getMaxAsyncPutMessageRequests();
		}

		public synchronized boolean putRequest(final GroupCommitRequest request) {
			synchronized (requestsWrite) {
				requestsWrite.add(request);
			}
			if (hasNotified.compareAndSet(false, true)) {
				waitPoint.countDown();
			}
			boolean flag = requestsWrite.size() > defaultMessageStore.getMessageStoreConfig().getMaxAsyncPutMessageRequests();
			if (flag) {
				log.info("Async requests {} exceeded the threshold {}", requestsWrite.size(), defaultMessageStore.getMessageStoreConfig().getMaxAsyncPutMessageRequests());
			}

			return flag;
		}

		private void swapRequests() {
			List<GroupCommitRequest> tmp = requestsWrite;
			requestsWrite = requestsRead;
			requestsRead = tmp;
		}

		private void doCommit() {
			synchronized (requestsRead) {
				if (!requestsRead.isEmpty()) {
					for (GroupCommitRequest req : requestsRead) {
						boolean flushOK = false;

						for (int i = 0; i < 1000; i++) {
							flushOK = mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
							if (flushOK) {
								break;
							}
							else {
								try {
									Thread.sleep(1);
								} catch (Throwable ignored) {}
							}
						}

						req.wakeupCustomer(flushOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
					}

					long storeTimestamp = mappedFileQueue.getStoreTimestamp();
					if (storeTimestamp > 0) {
						defaultMessageStore.getStoreCheckPoint().setPhysicMsgTimestamp(storeTimestamp);
					}

					requestsRead.clear();
				}
			}
		}

		@Override
		protected void onWaitEnd() {
			swapRequests();
		}

		@Override
		public long getJoinTime() {
			return 5 * 60 * 1000;
		}

		@Override
		public String getServiceName() {
			if (defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
				return defaultMessageStore.getBrokerConfig().getIdentifier() + GroupCheckService.class.getSimpleName();
			}
			return GroupCheckService.class.getSimpleName();
		}

		@Override
		public void run() {
			String serviceName = getServiceName();
			log.info("{} service started", serviceName);

			while (!isStopped()) {
				try {
					waitForRunning(1);
					doCommit();
				}
				catch (Exception e) {
					log.warn("{} service has exception.", serviceName, e);
				}
			}

			try {
				Thread.sleep(10);
			}
			catch (InterruptedException e) {
				log.warn("GroupCommitService Exception.", e);
			}

			synchronized (this) {
				swapRequests();
			}

			doCommit();

			log.info("{} service end", serviceName);
		}
	}

	class DefaultFlushManager implements FlushManager {

		private final FlushCommitLogService flushCommitLogService;

		private final FlushCommitLogService commitRealTimeService;

		public DefaultFlushManager() {
			if (FlushDiskType.SYNC_FLUSH == CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
				flushCommitLogService = new CommitLog.GroupCommitService();
			}
			else {
				flushCommitLogService = new CommitLog.FlushRealTimeService();
			}
			commitRealTimeService = new CommitLog.CommitRealTimeService();
		}

		@Override
		public void start() {
			flushCommitLogService.start();

			if (defaultMessageStore.isTransientStorePoolDeficient()) {
				commitRealTimeService.start();
			}
		}

		@Override
		public void shutdown() {
			if (defaultMessageStore.isTransientStorePoolDeficient()) {
				commitRealTimeService.shutdown();
			}

			flushCommitLogService.shutdown();
		}

		@Override
		public void wakeUpFlush() {
			flushCommitLogService.wakeup();
		}

		@Override
		public void wakeUpCommit() {
			commitRealTimeService.wakeup();
		}

		@Override
		public void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {

		}

		@Override
		public CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, MessageExt messageExt) {
			if (defaultMessageStore.getMessageStoreConfig().getFlushDiskType() == FlushDiskType.SYNC_FLUSH) {
				GroupCommitService service = (GroupCommitService) flushCommitLogService;
				if (messageExt.isWaitStoreMsgOK()) {
					GroupCommitRequest req = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(), defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
					FlushDiskWatcher.add(req);
					service.putRequest(req);
					return req.future();
				}
				else {
					service.wakeup();
					return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
				}
			}
			return null;
		}
	}
}
